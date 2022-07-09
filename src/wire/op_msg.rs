use crate::handler::{Request, Response};
use crate::wire::Replyable;
use crate::wire::{OpCode, UnknownMessageKindError, CHECKSUM_PRESENT, HEADER_SIZE, OP_MSG};
use bson::{doc, ser, Bson, Document};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{BufRead, Cursor, Read, Write};

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
    // pub identifier: u32,
    pub documents: Vec<Document>,
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
                documents: vec![doc.to_owned()],
            }],
            checksum,
        }
    }

    pub fn parse(header: MsgHeader, rdr: &mut Cursor<&[u8]>) -> OpMsg {
        let size = header.message_length as usize - 16;
        let mut body = vec![0; size];
        rdr.read_exact(&mut body).unwrap();

        let mut rdr = Cursor::new(&body);

        let flags = rdr.read_u32::<LittleEndian>().unwrap();
        let kind = rdr.read_u8().unwrap();

        // FIXME We're only handling kind 0 - and it only has one document
        let mut sections = vec![];

        // peek size of the document
        let size = rdr.read_u32::<LittleEndian>().unwrap();
        rdr.set_position(rdr.position() - 4);

        let mut rdr2 = rdr.clone();
        let doc = Document::from_reader(rdr).unwrap();
        let documents = vec![doc];
        rdr2.set_position(rdr2.position() + size as u64);

        sections.push(OpMsgSection { kind, documents });

        let checksum = if flags & CHECKSUM_PRESENT != 0 {
            Some(rdr2.read_u32::<LittleEndian>().unwrap())
        } else {
            None
        };

        OpMsg {
            header,
            flags,
            checksum,
            sections,
        }
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
                return Err(UnknownMessageKindError);
            }
        }

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
