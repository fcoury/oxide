use crate::handler::{handle, Response};
use crate::threadpool::ThreadPool;
use crate::wire::parse;
use autoincrement::prelude::AsyncIncremental;
use bson::{doc, Bson};
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
        Self::new_with_pgurl(listen_addr, port, env::var("DATABASE_URL").unwrap())
    }

    pub fn new_with_pgurl(listen_addr: String, port: u16, pg_url: String) -> Self {
        Server {
            listen_addr,
            port,
            pg_url,
        }
    }

    pub fn start(&self) {
        let manager = PostgresConnectionManager::new(self.pg_url.parse().unwrap(), NoTls);
        let pool = r2d2::Pool::new(manager).unwrap();
        self.start_with_pool(pool);
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
