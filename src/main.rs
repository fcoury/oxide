#![allow(dead_code)]
use autoincrement::prelude::*;
use oxide::ThreadPool;
use pretty_hex::*;
use std::io::prelude::*;
use std::net::Shutdown;
use std::net::TcpListener;
use std::net::TcpStream;

mod handler;
mod wire;

#[derive(AsyncIncremental, PartialEq, Eq, Debug)]
struct RequestId(u32);

fn main() {
  let listener = TcpListener::bind("127.0.0.1:37017").unwrap();
  let pool = ThreadPool::new(10);
  let generator = RequestId::init();

  for stream in listener.incoming() {
    let stream = stream.unwrap();
    let id = generator.pull();

    // stream.set_nodelay(true).unwrap();
    // stream.set_ttl(250).expect("set_ttl failed");

    pool.execute(|| {
      handle_connection(stream, id);
    });
  }

  println!("Shutting down.");
}

fn handle_connection(mut stream: TcpStream, id: RequestId) {
  let mut buffer = [0; 1024];
  loop {
    match stream.read(&mut buffer) {
      Ok(_read) => {
        println!(
          "*** Accepted connection from {}...",
          stream.peer_addr().unwrap(),
        );
        let op_msg = wire::parse_op_msg(&buffer);
        println!("*** Got message: {:?}", op_msg);
        let response = handler::handle(id.0, op_msg).unwrap();
        println!("*** Hex Dump:\n {}", pretty_hex(&response));

        stream.write(&response).unwrap();
        stream.flush().unwrap();
      }
      Err(e) => {
        println!("[{}-connection] Error: {}", id.0, e);
        stream.shutdown(Shutdown::Both).unwrap();
        return;
      }
    };
  }
}
