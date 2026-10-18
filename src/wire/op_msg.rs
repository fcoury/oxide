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
        if flags & !(super::MORE_TO_COME | super::EXHAUST_ALLOWED) != 0 {
            eyre::bail!("unsupported OP_MSG flags or checksum");
        }
        let mut offset = 20;
        let mut sections = Vec::new();
        let mut body = false;
        let mut identifiers = std::collections::HashSet::new();
        while offset < bytes.len() {
            if sections.len() == 4 {
                eyre::bail!("too many OP_MSG sections");
            }
            let (section, size) = parse_section(&bytes[offset..])?;
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
            checksum: None,
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
