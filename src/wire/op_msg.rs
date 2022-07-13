use crate::handler::{Request, Response};
use crate::utils::to_cstring;
use crate::wire::Replyable;
use crate::wire::{OpCode, UnknownMessageKindError, CHECKSUM_PRESENT, HEADER_SIZE, OP_MSG};
use bson::{doc, ser, Bson, Document};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use pretty_hex::pretty_hex;
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
        parse_section(&mut bytes)
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

    pub fn from_bytes(bytes: &[u8]) -> Result<OpMsg, UnknownMessageKindError> {
        let mut cursor = Cursor::new(bytes);
        let mut header_buffer: Vec<u8> = vec![0u8; HEADER_SIZE as usize];
        cursor.read_exact(&mut header_buffer).unwrap();

        let header = MsgHeader::from_bytes(header_buffer).unwrap();
        let flags = cursor.read_u32::<LittleEndian>().unwrap();

        let mut bytes: Vec<u8> = vec![];
        cursor.read_to_end(&mut bytes).unwrap();

        let mut sections = vec![];
        loop {
            let (section, remaining) = parse_section(&mut bytes).unwrap();
            bytes = remaining;
            sections.push(section);
            if bytes.is_empty() {
                break;
            }
            if (bytes.len() as u64) <= 4 {
                break;
            }
        }

        let mut checksum = None;
        if flags & CHECKSUM_PRESENT != 0 {
            checksum = Some(cursor.read_u32::<LittleEndian>().unwrap());
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
        // FIXME extract this serialization of a document to a helper
        let bson_vec = ser::to_vec(&res.get_doc()).unwrap();
        let bson_data: &[u8] = &bson_vec;
        let message_length = HEADER_SIZE + 5 + bson_data.len() as u32;

        if let OpCode::OpMsg(op_msg) = res.get_op_code().to_owned() {
            let header = op_msg.header.get_response(res.get_id(), message_length);

            if self.sections.len() > 0 && self.sections[0].kind == 0 {
                return Ok(OpMsg::new_with_body_kind(
                    header,
                    self.flags,
                    self.checksum,
                    res.get_doc(),
                )
                .to_vec());
            } else if self.sections.len() > 0 && self.sections[0].kind == 1 {
                return Ok(OpMsg::new_with_body_kind(
                    header,
                    self.flags,
                    self.checksum,
                    res.get_doc(),
                )
                .to_vec());
            }
        }

        log::error!(
            "Received unsupported section for msg = {:?}",
            res.get_op_code()
        );

        Err(UnknownMessageKindError)
    }
}

impl Serializable for OpMsg {
    fn to_vec(&self) -> Vec<u8> {
        let mut writer = Cursor::new(Vec::new());
        writer.write_all(&self.header.to_vec()).unwrap();
        writer.write_u32::<LittleEndian>(self.flags).unwrap();
        for section in &self.sections {
            writer.write(&[section.kind]).unwrap();
            for doc in &section.documents {
                let bson_vec = ser::to_vec(&doc).unwrap();
                let bson_data: &[u8] = &bson_vec;
                writer.write(bson_data).unwrap();
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
