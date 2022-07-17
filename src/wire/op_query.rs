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
    pub fn parse(header: MsgHeader, cursor: &mut Cursor<&[u8]>) -> OpQuery {
        let flags = cursor.read_u32::<LittleEndian>().unwrap();

        // collection is a CString
        let mut buffer: Vec<u8> = vec![];
        cursor.read_until(0, &mut buffer).unwrap();
        let collection = unsafe { CString::from_vec_unchecked(buffer) }
            .to_string_lossy()
            .to_string();

        let number_to_skip = cursor.read_u32::<LittleEndian>().unwrap();
        let number_to_return = cursor.read_u32::<LittleEndian>().unwrap();
        let mut new_cursor = cursor.clone();
        new_cursor.set_position(cursor.position());

        let len = cursor.get_ref().len();
        if (cursor.position() as usize) < len - 1 {
            return OpQuery {
                header,
                flags,
                collection,
                number_to_skip,
                number_to_return,
                query: doc! {},
                return_fields: None,
            };
        }

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

impl Replyable for OpQuery {
    fn reply(&self, res: Response) -> Result<Vec<u8>, UnknownMessageKindError> {
        // FIXME defer this logic to MsgHeader
        let bson_vec = ser::to_vec(&res.get_doc()).unwrap();
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

            return Ok(OpReply::new(
                header,
                self.flags,
                cursor_id,
                starting_from,
                number_returned,
                docs,
            )
            .to_vec());
        }
        Err(UnknownMessageKindError)
    }
}
