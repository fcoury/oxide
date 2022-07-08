#![allow(dead_code)]
use autoincrement::prelude::*;
use oxide::ThreadPool;
use std::io::prelude::*;
use std::net::Shutdown;
use std::net::TcpListener;
use std::net::TcpStream;

mod commands;
pub mod handler;
mod pg;
mod serializer;
pub mod wire;

#[derive(AsyncIncremental, PartialEq, Eq, Debug)]
struct RequestId(u32);

fn main() {
    dotenv::dotenv().ok();
    let listener = TcpListener::bind("127.0.0.1:37017").unwrap();
    let pool = ThreadPool::new(10);
    let generator = RequestId::init();

    println!("Server listening on port 37017...");

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        let id = generator.pull();

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
                    "\n*** Accepted connection from {}...",
                    stream.peer_addr().unwrap(),
                );
                // let op_msg = wire::OpMsg::parse(&buffer);
                // println!("*** Got message: {:?}", op_msg);
                let op_code = wire::parse(&buffer).unwrap();
                println!("*** Got message: {:?}", op_code);
                let response = handler::handle(id.0, op_code).unwrap();
                // println!("*** Hex Dump:\n {}", pretty_hex(&response));

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
