#![allow(dead_code)]
use mongodb::bson::Document;
use oxide::pg::PgDb;
use oxide::server::Server;
use r2d2_postgres::{postgres::NoTls, PostgresConnectionManager};
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{env, thread};

#[derive(Debug, Clone)]
pub struct TestContext {
    pub db: String,
    pub collection: String,
    mongodb: mongodb::sync::Client,
    port: u16,
}

impl TestContext {
    pub fn new(port: u16, db: String) -> Self {
        let id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let collection = format!("test_collection_{}", id).to_string();
        let client_uri = format!("mongodb://localhost:{}/test", port);
        let mongodb = mongodb::sync::Client::with_uri_str(&client_uri).unwrap();

        TestContext {
            db,
            collection,
            mongodb,
            port,
        }
    }

    pub fn mongodb(&self) -> &mongodb::sync::Client {
        &self.mongodb
    }

    pub fn db(&self) -> mongodb::sync::Database {
        self.mongodb().database(self.db.as_str())
    }

    pub fn col(&self) -> mongodb::sync::Collection<Document> {
        self.db().collection(self.collection.as_str())
    }

    pub fn send(&self, bytes: &[u8]) -> Vec<u8> {
        let mut stream = TcpStream::connect(&format!("localhost:{}", self.port)).unwrap();
        stream.write_all(bytes).unwrap();
        let mut buffer = [0u8; 1024];
        stream.read(&mut buffer).unwrap();
        stream.shutdown(Shutdown::Write).unwrap();

        buffer[..].to_vec()
    }
}

pub fn setup_with_pg_db(name: &str) -> TestContext {
    // static ID_COUNTER: AtomicU32 = AtomicU32::new(0);
    // let id = ID_COUNTER.fetch_add(1, Ordering::Relaxed);

    let _ = env_logger::builder().is_test(true).try_init();
    dotenv::dotenv().ok();

    PgDb::new().create_db_if_not_exists(name).unwrap();

    let pg_url = format!("{}/{}", env::var("TEST_DATABASE_URL").unwrap(), name);
    let port: u16 = portpicker::pick_unused_port().unwrap();

    let manager = PostgresConnectionManager::new(pg_url.parse().unwrap(), NoTls);
    let pool = r2d2::Pool::builder().max_size(2).build(manager).unwrap();
    PgDb::new_from_pool(pool.clone())
        .drop_schema("db_test")
        .unwrap();

    thread::spawn(move || {
        Server::new("localhost".to_string(), port).start_with_pool(pool);
    });

    TestContext::new(port, "db_test".to_string())
}

pub fn setup() -> TestContext {
    setup_with_pg_db("test")
}
