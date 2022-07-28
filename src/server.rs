use crate::handler::{handle, Response};
use crate::threadpool::ThreadPool;
use crate::wire::parse;
use autoincrement::prelude::AsyncIncremental;
use bson::{doc, Bson};
use byteorder::{ByteOrder, LittleEndian};
use r2d2_postgres::{postgres::NoTls, PostgresConnectionManager};
use std::env;
use std::io::prelude::*;
use std::net::{Shutdown, TcpListener, TcpStream};

#[derive(AsyncIncremental, PartialEq, Eq, Debug)]
struct RequestId(u32);

pub struct Server {
    listen_addr: String,
    port: u16,
    pg_url: String,
}

impl Server {
    pub fn new(listen_addr: String, port: u16) -> Self {
        Self::new_with_pgurl(
            listen_addr,
            port,
            env::var("DATABASE_URL").unwrap_or("postgres:://localhost:5432/oxide".to_string()),
        )
    }

    pub fn new_with_pgurl(listen_addr: String, port: u16, pg_url: String) -> Self {
        Server {
            listen_addr,
            port,
            pg_url,
        }
    }

    pub fn start(&self) {
        let uri = &self.pg_url;
        let sanitized_uri = format!(
            "postgres://*****:*****@{}",
            uri.split("@").collect::<Vec<_>>()[1]
        );
        log::info!("Connecting to {}...", sanitized_uri);
        let manager = PostgresConnectionManager::new(self.pg_url.parse().unwrap(), NoTls);
        if let Ok(pool) = r2d2::Pool::new(manager) {
            self.start_with_pool(pool);
        } else {
            log::error!("Failed to connect to PostgreSQL.");
        }
    }

    pub fn start_with_pool(&self, pg_pool: r2d2::Pool<PostgresConnectionManager<NoTls>>) {
        let addr = format!("{}:{}", self.listen_addr, self.port);
        let listener = TcpListener::bind(&addr).unwrap();
        let pool = ThreadPool::new(10);
        let generator = RequestId::init();

        log::info!("OxideDB listening on {}...", addr);
        for stream in listener.incoming() {
            let stream = stream.unwrap();
            let id = generator.pull();
            let pg_pool = pg_pool.clone();

            stream.set_nodelay(true).unwrap();

            pool.execute(|| {
                handle_connection(stream, id, pg_pool);
            });
        }

        log::info!("Shutting down.");
    }
}

fn handle_connection(
    mut stream: TcpStream,
    id: RequestId,
    pool: r2d2::Pool<PostgresConnectionManager<NoTls>>,
) {
    let addr = stream.peer_addr().unwrap();
    log::debug!("Client connected: {}", addr);

    loop {
        let mut size_buffer = [0; 4];
        let read = stream.peek(&mut size_buffer).unwrap();
        let size = LittleEndian::read_u32(&size_buffer);
        if size == 0 {
            stream.flush().unwrap();
            break;
        }
        let mut buffer = vec![0; size as usize];

        match stream.read_exact(&mut buffer) {
            Ok(_read) => {
                use std::time::Instant;
                let now = Instant::now();

                let op_code = parse(&buffer);
                log::trace!("{} {}bytes: {:?}", addr, read, op_code);
                if op_code.is_err() {
                    log::error!(
                        "Could not understand - {} {} bytes: {:?}",
                        addr,
                        read,
                        op_code
                    );
                    stream.write(&[0x00, 0x00, 0x00, 0x00]).unwrap();
                    stream.write(&[0x00, 0x00, 0x00, 0x00]).unwrap();
                    stream.write(&[0x00, 0x00, 0x00, 0x00]).unwrap();
                    stream.write(&[0x00, 0x00, 0x00, 0x00]).unwrap();
                    return;
                }

                let op_code = op_code.unwrap();
                let mut response = match handle(id.0, &pool, addr, &op_code) {
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
