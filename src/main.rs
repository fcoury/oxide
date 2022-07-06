#![allow(dead_code)]
use oxide::ThreadPool;
use pretty_hex::*;
use std::io::prelude::*;
use std::net::TcpListener;
use std::net::TcpStream;

mod handler;
mod wire;

fn main() {
  let listener = TcpListener::bind("127.0.0.1:37017").unwrap();
  let pool = ThreadPool::new(10);

  for stream in listener.incoming() {
    let stream = stream.unwrap();

    pool.execute(|| {
      handle_connection(stream);
    });
  }

  println!("Shutting down.");
}

fn handle_connection(mut stream: TcpStream) {
  let mut buffer = [0; 1024];
  stream.read(&mut buffer).unwrap();

  println!(
    "*** Accepted connection from {}...",
    stream.peer_addr().unwrap(),
  );
  let op_msg = wire::parse_op_msg(&buffer);
  println!("*** Got message: {:?}", op_msg);
  let response = handler::handle(op_msg).unwrap();
  println!(" *** Reply:\n {}", pretty_hex(&response));

  stream.write(&response).unwrap();
  stream.flush().unwrap();
}
