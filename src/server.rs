#![allow(dead_code)]
use bson::{doc, ser, Bson, Document};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use pretty_hex::*;
use std::io::{Cursor, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

const OP_MSG: u32 = 2013;
const OP_QUERY: u32 = 2004;

#[derive(Debug, Clone)]
struct UnknownCommandError;

#[derive(Debug, Clone)]
pub struct MsgHeader {
  message_length: u32,
  request_id: u32,
  response_to: u32,
  op_code: u32,
}

#[derive(Debug, PartialEq)]
pub struct OpMsgSection {
  kind: u8,
  // identifier: u32,
  documents: Vec<Document>,
}

#[derive(Debug)]
pub struct OpMsg {
  header: MsgHeader,
  flags: u32,
  checksum: u32,
  sections: Vec<OpMsgSection>,
}

fn parse_op_msg(data: &[u8]) -> OpMsg {
  let mut rdr = Cursor::new(&data);
  let header = MsgHeader {
    message_length: rdr.read_u32::<LittleEndian>().unwrap(),
    request_id: rdr.read_u32::<LittleEndian>().unwrap(),
    response_to: rdr.read_u32::<LittleEndian>().unwrap(),
    op_code: rdr.read_u32::<LittleEndian>().unwrap(),
  };
  println!("[request] {:?}", header);
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

      // FIXME what's the best way to advance to the previous cursor position?
      //       we were cloning the cursor before
      // let doc_rdr = rdr.clone();
      // let doc = Document::from_reader(rdr_clone).unwrap();
      // let new_pos = usize::try_from(rdr.position()).unwrap() + doc.len();
      // rdr.set_position(new_pos.try_into().unwrap());

      // FIXME checksum is not working because of the above issue
      // let checksum = rdr.read_u32::<LittleEndian>().unwrap();

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

fn handle(msg: OpMsg) -> Result<Document, UnknownCommandError> {
  let doc = msg.sections[0].documents[0].clone();
  let command = doc.keys().next().unwrap();
  println!("*** Command: {}", command);
  if command == "isMaster" {
    let local_time = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_millis();
    Ok(doc! {
      "ismaster": Bson::Boolean(true),
      "maxBsonObjectSize": MAX_DOCUMENT_LEN,
      "maxMessageSizeBytes": MAX_MSG_LEN,
      "maxWriteBatchSize": 100000,
      "localTime": Bson::Int64(local_time.try_into().unwrap()),
      "minWireVersion": 0,
      "maxWireVersion": 13,
      "readOnly": Bson::Boolean(false),
      "ok": Bson::Double(1.0)
    })
  } else {
    Err(UnknownCommandError)
  }
}

fn handle_client(id: u32, mut stream: TcpStream) {
  let mut data = [0; 1024];

  while match stream.read(&mut data) {
    Ok(size) => {
      if size < 1 {
        return;
      }
      let op_msg = parse_op_msg(&data[..size]);
      println!("[request] {:?}", op_msg);
      let request_id = id;
      let response_to = op_msg.header.request_id;
      let op_code = op_msg.header.op_code;
      let reply_doc = handle(op_msg).unwrap();
      let bson_vec = ser::to_vec(&reply_doc).unwrap();
      println!(" *** RAW =\n {}", pretty_hex(&bson_vec));
      let bson_data: &[u8] = &bson_vec;

      println!("reply_doc size = {:?}", bson_data.len());
      let res_header = MsgHeader {
        message_length: 16 + bson_data.len() as u32,
        request_id,
        response_to,
        op_code,
      };
      println!("[response] - {:?}", res_header);
      println!("[response] - {:?}", reply_doc);

      let mut res_data = Vec::new();
      res_data
        .write_u32::<LittleEndian>(res_header.message_length)
        .unwrap();
      res_data
        .write_u32::<LittleEndian>(res_header.request_id)
        .unwrap();
      res_data
        .write_u32::<LittleEndian>(res_header.response_to)
        .unwrap();
      res_data
        .write_u32::<LittleEndian>(res_header.op_code)
        .unwrap();

      let size = bson_data.len() + 4;
      res_data
        .write_u32::<LittleEndian>(size.try_into().unwrap())
        .unwrap();
      res_data.write_all(&vec![0]).unwrap();
      res_data.write_all(bson_data).unwrap();

      println!("{}", pretty_hex(&res_data));

      stream.write_all(&res_data).unwrap();
      stream.flush().unwrap();
      stream.shutdown(Shutdown::Both).unwrap();

      true
    }
    Err(_) => {
      println!(
        "An error occurred, terminating connection with {}",
        stream.peer_addr().unwrap()
      );
      stream.shutdown(Shutdown::Both).unwrap();
      false
    }
  } {}
}

fn main() {
  let listener = TcpListener::bind("0.0.0.0:37017").unwrap();
  let mut id = 1;
  // accept connections and process them, spawning a new thread for each one
  println!("Server listening on port 37017");
  for stream in listener.incoming() {
    match stream {
      Ok(stream) => {
        id += 1;
        println!(
          "[{}-connection] New connection: {}",
          id,
          stream.peer_addr().unwrap()
        );
        thread::spawn(move || handle_client(id, stream));
      }
      Err(e) => {
        println!("Error: {}", e);
        /* connection failed */
      }
    }
  }
  // close the socket server
  drop(listener);
}
