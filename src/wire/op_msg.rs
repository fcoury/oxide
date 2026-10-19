use crate::handler::{Request, Response};
use crate::wire::Replyable;
use crate::wire::{OpCode, UnknownMessageKindError, CHECKSUM_PRESENT, HEADER_SIZE, OP_MSG};
use bson::{doc, ser, Bson, Document};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::ffi::CString;
use std::io::{BufRead, Cursor, Read, Write};

use super::util::parse_section;
use super::{MsgHeader, Serializable};

#[derive(Debug, Clone)]
pub struct OpMsg {
    pub header: MsgHeader,
    pub flags: u32,
    pub sections: Vec<OpMsgSection>,
    pub checksum: Option<u32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OpMsgSection {
    pub kind: u8,
    pub identifier: Option<String>,
    pub documents: Vec<Document>,
}

impl OpMsgSection {
    pub fn from_bytes(
        mut bytes: Vec<u8>,
    ) -> Result<(OpMsgSection, Vec<u8>), UnknownMessageKindError> {
        let (section, size) = parse_section(&bytes)?;
        Ok((section, bytes.split_off(size)))
    }
}

impl OpMsg {
    pub fn new_with_body_kind(
        header: MsgHeader,
        flags: u32,
        checksum: Option<u32>,
        doc: &Document,
    ) -> OpMsg {
        OpMsg {
            header,
            flags,
            sections: vec![OpMsgSection {
                kind: 0,
                identifier: None,
                documents: vec![doc.to_owned()],
            }],
            checksum,
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> eyre::Result<OpMsg> {
        if bytes.len() < 21 {
            eyre::bail!("truncated OP_MSG");
        }
        let header = MsgHeader::from_bytes(bytes[..16].to_vec())?;
        if header.message_length as usize != bytes.len()
            || bytes.len() > super::MAX_MSG_LEN as usize
        {
            eyre::bail!("invalid OP_MSG length");
        }
        let flags = u32::from_le_bytes(bytes[16..20].try_into()?);
        if flags & !(CHECKSUM_PRESENT | super::MORE_TO_COME | super::EXHAUST_ALLOWED) != 0 {
            eyre::bail!("unsupported OP_MSG flags");
        }
        let (end, checksum) = if flags & CHECKSUM_PRESENT != 0 {
            let end = bytes
                .len()
                .checked_sub(4)
                .ok_or_else(|| eyre::eyre!("missing checksum"))?;
            if end < 21 {
                eyre::bail!("truncated checksummed OP_MSG");
            }
            let checksum = u32::from_le_bytes(bytes[end..].try_into()?);
            if crc32c::crc32c(&bytes[..end]) != checksum {
                eyre::bail!("OP_MSG checksum mismatch");
            }
            (end, Some(checksum))
        } else {
            (bytes.len(), None)
        };
        let mut offset = 20;
        let mut sections = Vec::new();
        let mut body = false;
        let mut identifiers = std::collections::HashSet::new();
        while offset < end {
            if sections.len() == 4 {
                eyre::bail!("too many OP_MSG sections");
            }
            let (section, size) = parse_section(&bytes[offset..end])?;
            if section.kind == 0 {
                if body {
                    eyre::bail!("duplicate command body");
                }
                body = true;
            } else if !identifiers.insert(section.identifier.clone()) {
                eyre::bail!("duplicate sequence identifier");
            }
            sections.push(section);
            offset += size;
        }
        if !body {
            eyre::bail!("missing command body");
        }
        Ok(OpMsg {
            header,
            flags,
            sections,
            checksum,
        })
    }
}

impl Replyable for OpMsg {
    fn reply(&self, res: Response) -> Result<Vec<u8>, UnknownMessageKindError> {
        let bson = ser::to_vec(res.get_doc())?;
        if bson.len() > super::MAX_DOCUMENT_LEN as usize {
            eyre::bail!("reply exceeds BSON limit");
        }
        let header = self
            .header
            .get_response(res.get_id(), HEADER_SIZE + 5 + bson.len() as u32);
        let mut bytes = header.to_vec();
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        bytes.push(0);
        bytes.extend(bson);
        Ok(bytes)
    }
}

impl Serializable for OpMsg {
    fn to_vec(&self) -> Vec<u8> {
        let mut writer = Cursor::new(Vec::new());
        writer.write_all(&self.header.to_vec()).unwrap();
        writer.write_u32::<LittleEndian>(self.flags).unwrap();
        for section in &self.sections {
            writer.write_all(&[section.kind]).unwrap();
            for doc in &section.documents {
                let bson_vec = ser::to_vec(&doc).unwrap();
                let bson_data: &[u8] = &bson_vec;
                writer.write_all(bson_data).unwrap();
            }
        }
        if (self.flags & CHECKSUM_PRESENT) != 0 {
            writer
                .write_u32::<LittleEndian>(self.checksum.unwrap())
                .unwrap();
        }
        writer.into_inner()
    }
}

#[cfg(test)]
mod checksum_tests {
    use super::*;

    fn frame(body: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&((25 + body.len()) as i32).to_le_bytes());
        bytes.extend_from_slice(&7_i32.to_le_bytes());
        bytes.extend_from_slice(&0_i32.to_le_bytes());
        bytes.extend_from_slice(&2013_i32.to_le_bytes());
        bytes.extend_from_slice(&CHECKSUM_PRESENT.to_le_bytes());
        bytes.push(0);
        bytes.extend_from_slice(body);
        bytes.extend_from_slice(&crc32c::crc32c(&bytes).to_le_bytes());
        bytes
    }

    #[test]
    fn accepts_crc32c_and_rejects_corruption_in_header_body_and_checksum() {
        assert_eq!(crc32c::crc32c(b"123456789"), 0xe3069283);
        let bytes = frame(&bson::to_vec(&doc! {"ping":1,"$db":"admin"}).unwrap());
        let parsed = OpMsg::from_bytes(&bytes).unwrap();
        assert!(parsed.checksum.is_some());
        assert_eq!(parsed.sections[0].documents[0].get_i32("ping").unwrap(), 1);
        for index in [4, 25, bytes.len() - 1] {
            let mut corrupted = bytes.clone();
            corrupted[index] ^= 1;
            assert!(OpMsg::from_bytes(&corrupted).is_err());
        }
    }

    #[test]
    fn bson_sections_cannot_consume_the_checksum_bytes() {
        let mut body = bson::to_vec(&doc! {"ping":1}).unwrap();
        let len = body.len() as i32 + 4;
        body[..4].copy_from_slice(&len.to_le_bytes());
        assert!(OpMsg::from_bytes(&frame(&body)).is_err());
    }
}
