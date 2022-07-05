#![allow(dead_code)]
use bson::{doc, Document};
use byteorder::{LittleEndian, ReadBytesExt};
use std::io::{Cursor, Read};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::thread;

#[derive(Debug)]
pub struct MsgHeader {
  message_length: u32,
  request_id: u32,
  respose_to: u32,
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
    respose_to: rdr.read_u32::<LittleEndian>().unwrap(),
    op_code: rdr.read_u32::<LittleEndian>().unwrap(),
  };
  println!("{:?}", header);
  let size = header.message_length as usize - 16;
  let mut body = vec![0; size];
  rdr.read_exact(&mut body).unwrap();
  println!("{:?}", body);

  match header.op_code {
    2013 => {
      let mut rdr = Cursor::new(&body);
      let flags = rdr.read_u32::<LittleEndian>().unwrap();
      println!("Flags: {:?}", flags);

      println!("POS: {:?} LEN: {:?}", rdr.position(), rdr.get_ref().len());
      let kind = rdr.read_u8().unwrap();
      println!("Kind: {:?}", kind);

      let mut sections = vec![];
      // while rdr.position() < rdr.get_ref().len().try_into().unwrap() {
      let doc_rdr = rdr.clone();
      let doc = Document::from_reader(doc_rdr).unwrap();
      println!("Doc: {:?}", doc);

      let new_pos = usize::try_from(rdr.position()).unwrap() + doc.len();
      rdr.set_position(new_pos.try_into().unwrap());

      let documents = vec![doc];
      sections.push(OpMsgSection { kind, documents });
      // }
      let checksum = rdr.read_u32::<LittleEndian>().unwrap();
      println!("Checksum: {:?}", checksum);
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

fn handle_client(mut stream: TcpStream) {
  let mut data = [0; 1024];
  while match stream.read(&mut data) {
    Ok(size) => {
      let op_msg = parse_op_msg(&data[..size]);

      println!("{:?}", op_msg);
      panic!("Ended");
      // true
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
  // accept connections and process them, spawning a new thread for each one
  println!("Server listening on port 37017");
  for stream in listener.incoming() {
    match stream {
      Ok(stream) => {
        println!("New connection: {}", stream.peer_addr().unwrap());
        thread::spawn(move || {
          // connection succeeded
          handle_client(stream)
        });
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
