use bson::{doc, ser, Bson, Document};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::ffi::CString;
use std::io::{BufRead, Cursor, Read, Write};

use crate::handler::{Request, Response};

use super::{
    MsgHeader, OpCode, OpReply, Replyable, Serializable, UnknownMessageKindError, HEADER_SIZE,
    OP_REPLY,
};

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

impl OpQuery {
    pub fn parse(header: MsgHeader, cursor: &mut Cursor<&[u8]>) -> eyre::Result<OpQuery> {
        let flags = cursor.read_u32::<LittleEndian>()?;
        let offset = cursor.position() as usize;
        let (collection, len) = super::util::cstring(&cursor.get_ref()[offset..])?;
        cursor.set_position((offset + len) as u64);
        let number_to_skip = cursor.read_u32::<LittleEndian>()?;
        let number_to_return = cursor.read_u32::<LittleEndian>()?;
        let (query, len) = super::util::document(&cursor.get_ref()[cursor.position() as usize..])?;
        cursor.set_position(cursor.position() + len as u64);
        let return_fields = if (cursor.position() as usize) < cursor.get_ref().len() {
            let (doc, len) =
                super::util::document(&cursor.get_ref()[cursor.position() as usize..])?;
            cursor.set_position(cursor.position() + len as u64);
            Some(doc)
        } else {
            None
        };
        if cursor.position() as usize != cursor.get_ref().len() {
            eyre::bail!("trailing OP_QUERY bytes");
        }
        Ok(OpQuery {
            header,
            flags,
            collection,
            number_to_skip,
            number_to_return,
            query,
            return_fields,
        })
    }
}

impl Replyable for OpQuery {
    fn reply(&self, res: Response) -> Result<Vec<u8>, UnknownMessageKindError> {
        // FIXME defer this logic to MsgHeader
        let bson_vec = ser::to_vec(res.get_doc())?;
        let bson_data: &[u8] = &bson_vec;
        let message_length = HEADER_SIZE + 20 + bson_data.len() as u32;

        if let OpCode::OpQuery(op_query) = res.get_op_code().to_owned() {
            let header =
                op_query
                    .header
                    .get_response_with_op_code(res.get_id(), message_length, OP_REPLY);
            let cursor_id = 0;
            let starting_from = 0;
            let number_returned = 1;
            let docs = vec![res.get_doc().to_owned()];

            return Ok(
                OpReply::new(header, 0, cursor_id, starting_from, number_returned, docs).to_vec(),
            );
        }
        Err(eyre::eyre!("unsupported OP_QUERY response"))
    }
}
