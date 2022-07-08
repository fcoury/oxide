#![allow(dead_code, unused_imports)]
use bson::{doc, ser, Bson, Document};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use pretty_hex::*;
use std::ffi::CString;
use std::io::{BufRead, Cursor, Read, Write};
use bitflags::bitflags;

pub const OP_MSG: u32 = 2013;
pub const OP_QUERY: u32 = 2004;
pub const MAX_DOCUMENT_LEN: u32 = 16777216;
pub const MAX_MSG_LEN: u32 = 48000000;
pub const HEADER_SIZE: u32 = 16;

pub const CHECKSUM_PRESENT: u32 = 1 << 0;
pub const MORE_TO_COME: u32 = 1 << 1;
pub const EXHAUST_ALLOWED: u32 = 1 << 16;

#[derive(Debug, Clone)]
pub struct UnknownCommandError;

#[derive(Debug, Clone)]
pub struct UnknownMessageKindError;

#[derive(Debug, Clone)]
pub struct MsgHeader {
    pub message_length: u32,
    pub request_id: u32,
    pub response_to: u32,
    pub op_code: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OpMsgSection {
    pub kind: u8,
    // pub identifier: u32,
    pub documents: Vec<Document>,
}

#[derive(Debug, Clone)]
pub struct OpMsg {
    pub header: MsgHeader,
    pub flags: u32,
    pub sections: Vec<OpMsgSection>,
    pub checksum: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct OpQuery {
    pub header: MsgHeader,
    pub flags: u32,
    pub collection: String,
    pub number_to_skip: u32,
    pub number_to_return: u32,
    pub query: Document,
    pub return_fields: Option<Document>,
}

#[derive(Debug, Clone)]
pub enum OpCode {
    OpMsg(OpMsg),
    OpQuery(OpQuery),
}

pub fn parse(buffer: &[u8]) -> Result<OpCode, UnknownCommandError> {
    let mut cursor = Cursor::new(buffer);
    let header = MsgHeader::parse(&mut cursor);
    if header.op_code == OP_MSG {
        let op_msg = OpMsg::parse(header, &mut cursor);
        Ok(OpCode::OpMsg(op_msg))
    } else if header.op_code == OP_QUERY {
        let op_query = OpQuery::parse(header, &mut cursor);
        Ok(OpCode::OpQuery(op_query))
    } else {
        Err(UnknownCommandError)
    }
}

impl MsgHeader {
    fn new(message_length: u32, request_id: u32, response_to: u32, op_code: u32) -> MsgHeader {
        MsgHeader {
            message_length,
            request_id,
            response_to,
            op_code,
        }
    }

    fn parse(cursor: &mut Cursor<&[u8]>) -> MsgHeader {
        let message_length = cursor.read_u32::<LittleEndian>().unwrap();
        let request_id = cursor.read_u32::<LittleEndian>().unwrap();
        let response_to = cursor.read_u32::<LittleEndian>().unwrap();
        let op_code = cursor.read_u32::<LittleEndian>().unwrap();
        MsgHeader {
            message_length,
            request_id,
            response_to,
            op_code,
        }
    }

    fn write(&self, cursor: &mut Cursor<&mut [u8]>) {
        cursor.write_u32::<LittleEndian>(header.message_length).unwrap();
        cursor.write_u32::<LittleEndian>(header.request_id).unwrap();
        cursor.write_u32::<LittleEndian>(header.response_to).unwrap();
        cursor.write_u32::<LittleEndian>(header.op_code).unwrap();
    }
}

impl OpQuery {
    pub fn parse(header: MsgHeader, cursor: &mut Cursor<&[u8]>) -> OpQuery {
        let flags = cursor.read_u32::<LittleEndian>().unwrap();
        // let collection = cursor.read_cstring().unwrap().to_string();
        let mut buffer: Vec<u8> = vec![];
        cursor.read_until(0, &mut buffer).unwrap();
        let collection = unsafe { CString::from_vec_unchecked(buffer) }
            .to_string_lossy()
            .to_string();
        let number_to_skip = cursor.read_u32::<LittleEndian>().unwrap();
        let number_to_return = cursor.read_u32::<LittleEndian>().unwrap();
        let mut new_cursor = cursor.clone();
        new_cursor.set_position(cursor.position());
        let query = Document::from_reader(cursor).unwrap();
        let bson_vec = ser::to_vec(&query).unwrap();
        let query_size: u64 = bson_vec.len().try_into().unwrap();
        new_cursor.set_position(new_cursor.position() + query_size);
        let return_fields = match Document::from_reader(new_cursor) {
            Ok(doc) => Some(doc),
            Err(_) => None,
        };
        OpQuery {
            header,
            flags,
            collection,
            number_to_skip,
            number_to_return,
            query,
            return_fields,
        }
    }
}

impl OpMsg {
    // pub fn new(request_id: u32, response_to: u32, doc: Document) -> OpMsg {
    //     let bson_vec = ser::to_vec(&doc).unwrap();
    //     let bson_data: &[u8] = &bson_vec;
    //     let message_length = HEADER_SIZE + 5 + bson_data.len() as u32;
    //     OpMsg {
    //         header: MsgHeader {
    //             message_length,
    //             request_id,
    //             response_to,
    //             op_code: OP_MSG,
    //         },
    //         flags: 0,
    //         sections: vec![OpMsgSection {
    //             kind: 0,
    //             documents: vec![doc],
    //         }],
    //         checksum: 0,
    //     }
    // }

    pub fn new_with_body_kind(header: MsgHeader, flags: u32, checksum: Option<u32>, doc: Document) -> OpMsg {
        let bson_vec = ser::to_vec(&doc).unwrap();
        let bson_data: &[u8] = &bson_vec;
        let message_length = HEADER_SIZE + 5 + bson_data.len() as u32;

        OpMsg {
            header,
            flags,
            sections: vec![OpMsgSection {
                kind: 0,
                documents: vec![doc],
            }],
            checksum,
        }
    }

    pub fn parse(header: MsgHeader, rdr: &mut Cursor<&[u8]>) -> OpMsg {
        let size = header.message_length as usize - 16;
        let mut body = vec![0; size];
        rdr.read_exact(&mut body).unwrap();

        match header.op_code {
            OP_MSG => {
                let mut rdr = Cursor::new(&body);

                let flags = rdr.read_u32::<LittleEndian>().unwrap();
                let kind = rdr.read_u8().unwrap();

                // FIXME We're only handling kind 0 - and it only has one document
                let mut sections = vec![];

                let doc = Document::from_reader(rdr).unwrap();
                let documents = vec![doc];
                sections.push(OpMsgSection { kind, documents });

                let checksum = if flags & CHECKSUM_PRESENT != 0 {
                    Some(rdr.read_u32::<LittleEndian>().unwrap())
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
            _ => OpMsg {
                header,
                flags: 0,
                checksum: Some(0),
                sections: vec![],
            },
        }
    }

    pub fn reply(&self, request_id: u32, msg: OpMsg, doc: Document) -> Result<OpMsg, UnknownMessageKindError> {
        let bson_vec = ser::to_vec(&doc).unwrap();
        let bson_data: &[u8] = &bson_vec;
        let message_length = HEADER_SIZE + 5 + bson_data.len() as u32;

        let header = MsgHeader::new(message_length, request_id, 0, OP_MSG);

        if msg.sections.len() > 0 && msg.sections[0].kind == 0 {
            return Ok(OpMsg::new_with_body_kind(header, msg.flags, msg.checksum, doc));
        } else if msg.sections.len() > 0 && msg.sections[0].kind == 1 {
            return Err(UnknownMessageKindError);
        }

        Err(UnknownMessageKindError)
    }

    pub fn write(&self, cursor: &mut Cursor<&mut [u8]>) {
        // self.header.write(cursor);
        // cursor.write_u32::<LittleEndian>(self.flags).unwrap();
        // cursor.write_u8(self.sections.len() as u8).unwrap();
        // for section in &self.sections {
        //     cursor.write_u8(section.kind).unwrap();
        //     cursor.write_u32::<LittleEndian>(section.documents.len() as u32).unwrap();
        //     for doc in &section.documents {
        //         ser::to_writer(cursor, doc).unwrap();
        //     }
        // }
        // cursor.write_u32::<LittleEndian>(self.checksum).unwrap();
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn test_parse_op_msg() {
//         let doc = doc! {
//           "a": 1,
//           "b": 2,
//           "c": 3,
//         };

//         let data = vec![
//             0x00u8, 0x00, 0x00, 0x00, // message_length
//             0x00, 0x00, 0x00, 0x00, // request_id
//             0x00, 0x00, 0x00, 0x00, // response_to
//             0xDD, 0x07, 0x00, 0x00, // op_code
//             0x00, 0x00, 0x00, 0x00, // flags
//             0x00, // kind
//         ];

//         let bson_vec = ser::to_vec(&doc).unwrap();
//         let mut res = data
//             .into_iter()
//             .chain(bson_vec.into_iter())
//             .collect::<Vec<u8>>();
//         res[0] = res.len() as u8;

//         let mut cursor = &mut Cursor::new(res);
//         let header = MsgHeader::parse(&mut cursor);
//         let op_msg = OpMsg::parse(&mut cursor, &res);
//         println!("{:?}", op_msg);
//         assert_eq!(op_msg.header.message_length, 47);
//         assert_eq!(op_msg.header.request_id, 0);
//         assert_eq!(op_msg.header.response_to, 0);
//         assert_eq!(op_msg.header.op_code, OP_MSG);
//         assert_eq!(op_msg.flags, 0);
//         assert_eq!(op_msg.checksum, 0);
//         assert_eq!(op_msg.sections[0].documents[0], doc);
//     }
// }
