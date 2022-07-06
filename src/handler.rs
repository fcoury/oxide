use crate::commands::{Handler, IsMaster, ListDatabases};
use crate::wire::{OpMsg, UnknownCommandError, HEADER_SIZE};
use bson::{ser, Document};
use byteorder::{LittleEndian, WriteBytesExt};
use std::io::Write;
// use std::time::{SystemTime, UNIX_EPOCH};

const MAX_DOCUMENT_LEN: u32 = 16777216;
const MAX_MSG_LEN: u32 = 48000000;

pub fn handle(request_id: u32, msg: OpMsg) -> Result<Vec<u8>, UnknownCommandError> {
  match route(&msg) {
    Ok(doc) => {
      let bson_vec = ser::to_vec(&doc).unwrap();
      let bson_data: &[u8] = &bson_vec;

      let mut res_data = Vec::new();
      let header = &msg.header;
      let message_size = HEADER_SIZE + 5 + bson_data.len() as u32;

      println!(
        "*** Response: msgsize={} requestid={} responseto={} opcode={}",
        message_size, request_id, header.request_id, header.op_code
      );
      println!("*** Response document: {:?}", doc);

      // header
      res_data.write_u32::<LittleEndian>(message_size).unwrap();
      res_data.write_u32::<LittleEndian>(request_id).unwrap();
      res_data
        .write_u32::<LittleEndian>(header.request_id)
        .unwrap();
      res_data.write_u32::<LittleEndian>(header.op_code).unwrap();

      // FIXME flagbits
      res_data.write_u32::<LittleEndian>(0).unwrap();

      // sections
      // FIXME section kind
      res_data.write_all(&[0]).unwrap();

      // section contents
      res_data.write_all(bson_data).unwrap();

      Ok(res_data)
    }
    Err(e) => Err(e),
  }
}

fn route(msg: &OpMsg) -> Result<Document, UnknownCommandError> {
  let doc = msg.sections[0].documents[0].clone();
  let command = doc.keys().next().unwrap();
  println!("*** Command: {}", command);
  if command == "isMaster" {
    IsMaster::new().handle(doc)
  } else if command == "listDatabases" {
    ListDatabases::new().handle(doc)
  } else {
    println!("Got unknown command: {}", command);
    Err(UnknownCommandError)
  }
}
