#![allow(dead_code, unused_imports)]
use bson::{doc, ser, Bson, Document};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use pretty_hex::*;
use std::io::{Cursor, Read, Write};

const OP_MSG: u32 = 2013;
const OP_QUERY: u32 = 2004;
const MAX_DOCUMENT_LEN: u32 = 16777216;
const MAX_MSG_LEN: u32 = 48000000;
pub const HEADER_SIZE: u32 = 16;

#[derive(Debug, Clone)]
pub struct UnknownCommandError;

#[derive(Debug)]
pub struct MsgHeader {
  pub message_length: u32,
  pub request_id: u32,
  pub response_to: u32,
  pub op_code: u32,
}

#[derive(Debug, PartialEq)]
pub struct OpMsgSection {
  pub kind: u8,
  // pub identifier: u32,
  pub documents: Vec<Document>,
}

#[derive(Debug)]
pub struct OpMsg {
  pub header: MsgHeader,
  pub flags: u32,
  pub sections: Vec<OpMsgSection>,
  pub checksum: u32,
}

pub fn parse_op_msg(data: &[u8]) -> OpMsg {
  let mut rdr = Cursor::new(&data);
  let message_length = rdr.read_u32::<LittleEndian>().unwrap();
  let request_id = rdr.read_u32::<LittleEndian>().unwrap();
  let response_to = rdr.read_u32::<LittleEndian>().unwrap();
  let op_code = rdr.read_u32::<LittleEndian>().unwrap();

  let header = MsgHeader {
    message_length,
    request_id,
    response_to,
    op_code,
  };

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

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_parse_op_msg() {
    let doc = doc! {
      "a": 1,
      "b": 2,
      "c": 3,
    };

    let data = vec![
      0x00u8, 0x00, 0x00, 0x00, // message_length
      0x00, 0x00, 0x00, 0x00, // request_id
      0x00, 0x00, 0x00, 0x00, // response_to
      0xDD, 0x07, 0x00, 0x00, // op_code
      0x00, 0x00, 0x00, 0x00, // flags
      0x00, // kind
    ];

    let bson_vec = ser::to_vec(&doc).unwrap();
    let mut res = data
      .into_iter()
      .chain(bson_vec.into_iter())
      .collect::<Vec<u8>>();
    res[0] = res.len() as u8;

    let op_msg = parse_op_msg(&res);
    println!("{:?}", op_msg);
    assert_eq!(op_msg.header.message_length, 47);
    assert_eq!(op_msg.header.request_id, 0);
    assert_eq!(op_msg.header.response_to, 0);
    assert_eq!(op_msg.header.op_code, OP_MSG);
    assert_eq!(op_msg.flags, 0);
    assert_eq!(op_msg.checksum, 0);
    assert_eq!(op_msg.sections[0].documents[0], doc);
  }
}
