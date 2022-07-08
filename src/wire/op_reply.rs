use bson::{doc, ser, Bson, Document};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{BufRead, Cursor, Read, Write};

use super::{MsgHeader, Serializable};

#[derive(Debug, Clone)]
pub struct OpReply {
    pub header: MsgHeader,
    pub flags: u32,
    pub cursor_id: u64,
    pub starting_from: u32,
    pub number_returned: u32,
    pub documents: Vec<Document>,
}

impl OpReply {
    pub fn new(
        header: MsgHeader,
        flags: u32,
        cursor_id: u64,
        starting_from: u32,
        number_returned: u32,
        documents: Vec<Document>,
    ) -> Self {
        OpReply {
            header,
            flags,
            cursor_id,
            starting_from,
            number_returned,
            documents,
        }
    }
}

impl Serializable for OpReply {
    fn to_vec(&self) -> Vec<u8> {
        let mut writer = Cursor::new(Vec::new());
        writer.write_all(&self.header.to_vec()).unwrap();
        writer.write_u32::<LittleEndian>(self.flags).unwrap();
        writer.write_u64::<LittleEndian>(self.cursor_id).unwrap();
        writer
            .write_u32::<LittleEndian>(self.starting_from)
            .unwrap();
        writer
            .write_u32::<LittleEndian>(self.number_returned)
            .unwrap();

        // FIXME support multiple documents
        let bson_vec = ser::to_vec(&self.documents[0]).unwrap();
        let bson_data: &[u8] = &bson_vec;
        writer.write(bson_data).unwrap();

        writer.into_inner()
    }
}
