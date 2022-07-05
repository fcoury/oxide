#![allow(dead_code)]
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

fn handle_client(mut stream: TcpStream) {
  let mut data = [0; 1024];
  while match stream.read(&mut data) {
    Ok(size) => {
      let mut rdr = Cursor::new(&data[..size]);
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
