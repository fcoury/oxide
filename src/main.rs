use autoincrement::prelude::*;
use oxide::ThreadPool;
use pretty_hex::pretty_hex;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter};
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

        stream.set_nodelay(true).unwrap();

        pool.execute(|| {
            handle_connection(stream, id);
            println!("Done.")
        });
    }

    println!("Shutting down.");
}

fn handle_connection(mut stream: TcpStream, id: RequestId) {
    let mut buffer = [0; 1204];
    let addr = stream.peer_addr().unwrap();
    println!("\n*** Accepted connection from {}...", addr);

    // let mut reader = BufReader::new(stream.try_clone().unwrap());
    // let mut writer = BufWriter::new(stream);
    loop {
        match stream.read(&mut buffer) {
            Ok(read) => {
                println!("read = {}", read);
                if read < 1 {
                    stream.flush().unwrap();
                    println!("Flushed!");
                    break;
                }

                use std::time::Instant;
                let now = Instant::now();

                let op_code = wire::parse(&buffer).unwrap();
                println!("*** Got message: {:?}", op_code);

                // let mut buf: Vec<u8> = vec![];
                // stream.read_to_end(&mut buf).unwrap();
                // println!("*** Read after:\n {}", pretty_hex(&buf));

                let mut response = handler::handle(id.0, op_code).unwrap();
                response.flush().unwrap();
                println!("*** Hex Dump:\n {}", pretty_hex(&response));

                let elapsed = now.elapsed();
                println!("Elapsed: {:.2?}", elapsed);
                println!("Response size: {}", response.len());
                stream.write_all(&response).unwrap();
                println!("Response sent!");
                // stream.shutdown(Shutdown::Both).unwrap();
            }
            Err(e) => {
                println!("[{}-connection] Error: {}", id.0, e);
                // stream.shutdown(Shutdown::Both).unwrap();
                return;
            }
        };
    }
}
