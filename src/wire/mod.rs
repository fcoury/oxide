#![allow(dead_code, unused_imports)]
use bson::{doc, ser, Bson, Document};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
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

pub type UnknownMessageKindError = eyre::Report;

#[derive(Debug, Clone)]
pub struct MsgHeader {
    pub message_length: u32,
    pub request_id: u32,
    pub response_to: u32,
    pub op_code: u32,
}

impl MsgHeader {
    pub fn from_bytes(bytes: Vec<u8>) -> eyre::Result<MsgHeader> {
        if bytes.len() < HEADER_SIZE as usize {
            eyre::bail!("truncated message header");
        }
        let mut cursor = Cursor::new(bytes);
        let message_length = cursor.read_u32::<LittleEndian>()?;
        let request_id = cursor.read_u32::<LittleEndian>()?;
        let response_to = cursor.read_u32::<LittleEndian>()?;
        let op_code = cursor.read_u32::<LittleEndian>()?;
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
            OpCode::OpMsg(op_msg) => op_msg.reply(response),
            OpCode::OpQuery(op_query) => op_query.reply(response),
            _ => {
                log::error!("Unknown message during reply - {:#?}", self);
                Err(eyre::eyre!("unsupported reply opcode"))
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

pub fn parse(buffer: &[u8]) -> eyre::Result<OpCode> {
    let header = MsgHeader::from_bytes(
        buffer
            .get(..16)
            .ok_or_else(|| eyre::eyre!("truncated header"))?
            .to_vec(),
    )?;
    if header.message_length < HEADER_SIZE
        || header.message_length > MAX_MSG_LEN
        || header.message_length as usize != buffer.len()
    {
        eyre::bail!("invalid message length");
    }
    match header.op_code {
        OP_MSG => Ok(OpCode::OpMsg(OpMsg::from_bytes(buffer)?)),
        OP_QUERY => {
            let mut cursor = Cursor::new(buffer);
            cursor.set_position(16);
            Ok(OpCode::OpQuery(OpQuery::parse(header, &mut cursor)?))
        }
        _ => eyre::bail!("unsupported opcode {}", header.op_code),
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
