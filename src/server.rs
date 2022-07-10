use crate::handler::{handle, Response};
use crate::wire::parse;
use autoincrement::prelude::*;
use bson::{doc, Bson};
use oxide::ThreadPool;
use std::io::prelude::*;
use std::net::{Shutdown, TcpListener, TcpStream};

#[derive(AsyncIncremental, PartialEq, Eq, Debug)]
struct RequestId(u32);

pub struct Server {
    listen_addr: String,
    port: u16,
}

impl Server {
    pub fn new(listen_addr: String, port: u16) -> Self {
        Server { listen_addr, port }
    }

    pub fn start(&self) {
        let addr = format!("{}:{}", self.listen_addr, self.port);
        let listener = TcpListener::bind(&addr).unwrap();
        let pool = ThreadPool::new(10);
        let generator = RequestId::init();

        log::info!("OxideDB listening on {}...", addr);
        for stream in listener.incoming() {
            let stream = stream.unwrap();
            let id = generator.pull();

            stream.set_nodelay(true).unwrap();

            pool.execute(|| {
                handle_connection(stream, id);
            });
        }

        log::info!("Shutting down.");
    }
}

fn handle_connection(mut stream: TcpStream, id: RequestId) {
    let mut buffer = [0; 1204];
    let addr = stream.peer_addr().unwrap();

    loop {
        match stream.read(&mut buffer) {
            Ok(read) => {
                if read < 1 {
                    stream.flush().unwrap();
                    break;
                }

                use std::time::Instant;
                let now = Instant::now();

                let op_code = parse(&buffer).unwrap();
                log::trace!("{} {}bytes: {:?}", addr, read, op_code);

                let mut response = match handle(id.0, addr, &op_code) {
                    Ok(reply) => reply,
                    Err(e) => {
                        log::error!("Error while handling: {}", e);
                        let err = doc! {
                            "ok": Bson::Double(0.0),
                            "errmsg": Bson::String(format!("{}", e)),
                            "code": Bson::Int32(59),
                            "codeName": "CommandNotFound",
                        };
                        let request = Response::new(id.0, &op_code, vec![err]);
                        op_code.reply(request).unwrap()
                    }
                };

                response.flush().unwrap();

                let elapsed = now.elapsed();
                log::trace!("Processed {}bytes in {:.2?}\n", response.len(), elapsed);
                stream.write_all(&response).unwrap();
            }
            Err(e) => {
                log::error!("Error on request id {}: {}", id.0, e);
                stream.shutdown(Shutdown::Both).unwrap();
                return;
            }
        };
    }
}
