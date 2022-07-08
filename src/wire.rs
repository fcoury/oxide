#![allow(dead_code, unused_imports)]
use bson::{doc, ser, Bson, Document};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use pretty_hex::*;
use std::ffi::CString;
use std::io::{BufRead, Cursor, Read, Write};

pub const OP_MSG: u32 = 2013;
pub const OP_QUERY: u32 = 2004;
pub const MAX_DOCUMENT_LEN: u32 = 16777216;
pub const MAX_MSG_LEN: u32 = 48000000;
pub const HEADER_SIZE: u32 = 16;

#[derive(Debug, Clone)]
pub struct UnknownCommandError;

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
    pub checksum: u32,
}

#[derive(Debug, Clone)]
pub struct OpQuery {
    pub header: MsgHeader,
    pub flags: u32,
    pub collection: String,
    pub number_to_skip: u32,
    pub number_to_return: u32,
    // pub query: Document,
    // pub return_fields: Option<Document>,
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
        // let query = doconvert_bson(cursor).unwrap();
        // let return_fields = if cursor.position() < cursor.get_ref().len() {
        //     Some(doconvert_bson(cursor).unwrap())
        // } else {
        //     None
        // };
        OpQuery {
            header,
            flags,
            collection,
            number_to_skip,
            number_to_return,
            // query,
            // return_fields,
        }
    }
}

impl OpMsg {
    pub fn new_with_body_kind(doc: Document) -> OpMsg {
        let bson_vec = ser::to_vec(&doc).unwrap();
        let bson_data: &[u8] = &bson_vec;
        let message_length = 16 + bson_data.len() as u32;
        OpMsg {
            header: MsgHeader {
                message_length,
                request_id: 0,
                response_to: 0,
                op_code: OP_MSG,
            },
            flags: 0,
            sections: vec![OpMsgSection {
                kind: 0,
                documents: vec![doc],
            }],
            checksum: 0,
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

                let checksum = 0;
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
                checksum: 0,
                sections: vec![],
            },
        }
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
