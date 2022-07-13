#![allow(dead_code, unused_imports)]
use bson::{doc, ser, Bson, Document};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use pretty_hex::*;
use std::ffi::CString;
use std::io::{BufRead, Cursor, Read, Write};

mod op_msg;
mod op_query;
mod op_reply;
mod util;

use crate::handler::{Request, Response};

pub use self::op_msg::OpMsg;
pub use self::op_msg::OpMsgSection;
pub use self::op_query::OpQuery;
pub use self::op_reply::OpReply;

// OpCodes
pub const OP_MSG: u32 = 2013;
pub const OP_REPLY: u32 = 1;
pub const OP_QUERY: u32 = 2004;

pub const MAX_DOCUMENT_LEN: u32 = 16777216;
pub const MAX_MSG_LEN: u32 = 48000000;
pub const HEADER_SIZE: u32 = 16;

pub const CHECKSUM_PRESENT: u32 = 1 << 0;
pub const MORE_TO_COME: u32 = 1 << 1;
pub const EXHAUST_ALLOWED: u32 = 1 << 16;

#[derive(Debug, Clone)]
pub struct OpCodeNotImplementedError {
    op_code: u32,
}

#[derive(Debug, Clone)]
pub struct UnknownMessageKindError;

#[derive(Debug, Clone)]
pub struct MsgHeader {
    pub message_length: u32,
    pub request_id: u32,
    pub response_to: u32,
    pub op_code: u32,
}

impl MsgHeader {
    pub fn from_bytes(bytes: Vec<u8>) -> Result<MsgHeader, UnknownMessageKindError> {
        let mut cursor = Cursor::new(bytes);
        let message_length = cursor.read_u32::<LittleEndian>().unwrap();
        let request_id = cursor.read_u32::<LittleEndian>().unwrap();
        let response_to = cursor.read_u32::<LittleEndian>().unwrap();
        let op_code = cursor.read_u32::<LittleEndian>().unwrap();
        Ok(MsgHeader {
            message_length,
            request_id,
            response_to,
            op_code,
        })
    }

    pub fn get_response(&self, request_id: u32, message_length: u32) -> MsgHeader {
        self.get_response_with_op_code(request_id, message_length, self.op_code)
    }

    pub fn get_response_with_op_code(
        &self,
        request_id: u32,
        message_length: u32,
        op_code: u32,
    ) -> MsgHeader {
        MsgHeader {
            message_length,
            request_id,
            response_to: self.request_id,
            op_code,
        }
    }
}

#[derive(Debug, Clone)]
pub enum OpCode {
    OpMsg(OpMsg),
    OpQuery(OpQuery),
    OpReply(OpReply),
}

impl OpCode {
    pub fn reply(&self, response: Response) -> Result<Vec<u8>, UnknownMessageKindError> {
        match self {
            OpCode::OpMsg(op_msg) => Ok(op_msg.reply(response).unwrap()),
            OpCode::OpQuery(op_query) => Ok(op_query.reply(response).unwrap()),
            _ => {
                log::error!("Unknown message during reply - {:#?}", self);
                Err(UnknownMessageKindError)
            }
        }
    }
}

pub trait Replyable {
    fn reply(&self, response: Response) -> Result<Vec<u8>, UnknownMessageKindError>
    where
        Self: Sized;
}

pub trait Serializable {
    fn to_vec(&self) -> Vec<u8>;
}

pub fn parse(buffer: &[u8]) -> Result<OpCode, OpCodeNotImplementedError> {
    let mut cursor = Cursor::new(buffer);
    let header = MsgHeader::parse(&mut cursor);
    if header.op_code == OP_MSG {
        Ok(OpCode::OpMsg(OpMsg::parse(header, &mut cursor)))
    } else if header.op_code == OP_QUERY {
        Ok(OpCode::OpQuery(OpQuery::parse(header, &mut cursor)))
    } else {
        Err(OpCodeNotImplementedError {
            op_code: header.op_code,
        })
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

    fn to_vec(&self) -> Vec<u8> {
        let mut cursor = Cursor::new(Vec::new());
        cursor
            .write_u32::<LittleEndian>(self.message_length)
            .unwrap();
        cursor.write_u32::<LittleEndian>(self.request_id).unwrap();
        cursor.write_u32::<LittleEndian>(self.response_to).unwrap();
        cursor.write_u32::<LittleEndian>(self.op_code).unwrap();
        cursor.into_inner()
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
