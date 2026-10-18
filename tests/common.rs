#![allow(dead_code)]
use bson::Bson;
use mongodb::bson::Document;
use mongodb::sync::Cursor;
use oxide::pg::PgDb;
use oxide::server::Server;
use r2d2::Pool;
use r2d2_postgres::{postgres::NoTls, PostgresConnectionManager};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{env, thread};

static SERVERS: OnceLock<Mutex<HashMap<String, (u16, mongodb::sync::Client)>>> = OnceLock::new();

#[derive(Debug, Clone)]
pub struct TestContext {
    pub db: String,
    pub collection: String,
    mongodb: mongodb::sync::Client,
    port: u16,
}

impl TestContext {
    pub fn new(port: u16, db: String) -> Self {
        let id = uuid::Uuid::new_v4().to_string();
        let collection = format!("test_{}", id).to_string();
        let client_uri = format!("mongodb://localhost:{}/test", port);
        let mongodb = mongodb::sync::Client::with_uri_str(&client_uri).unwrap();

        TestContext {
            db,
            collection,
            mongodb,
            port,
        }
    }

    pub fn port(&self) -> u16 {
        self.port
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
        stream.flush().unwrap();
        let mut length = [0u8; 4];
        stream.read_exact(&mut length).unwrap();
        let size = i32::from_le_bytes(length) as usize;
        assert!((16..=48_000_000).contains(&size));
        let mut buffer = vec![0; size];
        buffer[..4].copy_from_slice(&length);
        stream.read_exact(&mut buffer[4..]).unwrap();
        stream.shutdown(Shutdown::Write).unwrap();
        buffer
    }

    pub fn send_file(&self, filename: &str) -> Vec<u8> {
        let mut f = File::open(&filename).unwrap();
        let metadata = fs::metadata(&filename).unwrap();
        println!("file size = {}", metadata.len());
        let mut buffer = vec![0; metadata.len() as usize];
        f.read(&mut buffer).unwrap();

        self.send(&buffer)
    }
}

// One server and driver pool per PostgreSQL database, not per test. Tests
// isolate collections; the suite must point at a disposable PostgreSQL instance.
pub fn setup_with_pg_db(name: &str, drop: bool) -> TestContext {
    let _ = env_logger::builder().is_test(true).try_init();
    let mut servers = SERVERS.get_or_init(Default::default).lock().unwrap();
    let (port, mongodb) = servers.entry(name.into()).or_insert_with(|| {
        let name = format!("{name}_{}", std::process::id());
        if drop {
            PgDb::try_with_uri(&std::env::var("DATABASE_URL").unwrap())
                .unwrap()
                .drop_db(&name)
                .unwrap();
        }
        PgDb::try_with_uri(&std::env::var("DATABASE_URL").unwrap())
            .unwrap()
            .create_db_if_not_exists(&name)
            .unwrap();
        let pg_url = format!(
            "{}/{}",
            env::var("TEST_DATABASE_URL")
                .expect("set TEST_DATABASE_URL to a disposable PostgreSQL instance"),
            name
        );
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let manager = PostgresConnectionManager::new(pg_url.parse().unwrap(), NoTls);
        let pool = Pool::builder().max_size(4).build(manager).unwrap();
        thread::spawn(move || {
            Server::new("127.0.0.1".into(), port)
                .serve(listener, pool)
                .unwrap()
        });
        let client = mongodb::sync::Client::with_uri_str(format!(
            "mongodb://127.0.0.1:{port}/?maxPoolSize=8&serverSelectionTimeoutMS=5000"
        ))
        .unwrap();
        (port, client)
    });
    TestContext {
        db: "db_test".into(),
        collection: format!("test_{}", uuid::Uuid::new_v4()),
        mongodb: mongodb.clone(),
        port: *port,
    }
}

pub fn setup() -> TestContext {
    setup_with_pg_db("db_test", false)
}

pub fn setup_with_drop(drop: bool) -> TestContext {
    setup_with_pg_db("test", drop)
}

pub fn get_rows(cursor: Cursor<Document>) -> Vec<Document> {
    let rows: Vec<Result<Document, mongodb::error::Error>> = cursor.collect();
    let rows: Result<Vec<Document>, mongodb::error::Error> = rows.into_iter().collect();
    rows.unwrap()
}

pub fn get_datetime(rfc3339: &str) -> Bson {
    let date = chrono::DateTime::parse_from_rfc3339(rfc3339).unwrap();
    let time: u128 = SystemTime::from(date)
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    Bson::DateTime(bson::DateTime::from_millis(time.try_into().unwrap()))
}

#[macro_export]
macro_rules! insert {
    ( $( $x:expr ),+ $(,)? ) => {
        {
            let ctx = common::setup();
            ctx.col()
                .insert_many(vec![
                        $( $x, )*
                    ]).run()
                .unwrap();
            ctx.col()
        }
    };
}

#[macro_export]
macro_rules! assert_row_count {
    ( $col:expr, $query:expr, $exp:expr ) => {{
        let cursor = $col.find($query).run().unwrap();
        let rows: Vec<Result<Document, mongodb::error::Error>> = cursor.collect();
        assert_eq!($exp, rows.len());
    }};
}

#[macro_export]
macro_rules! assert_unique_row_value {
    ( $cursor:expr, $field:expr, $value:expr ) => {{
        use mongodb::bson::Document;
        use std::any::Any;

        let rows: Vec<Result<Document, mongodb::error::Error>> = $cursor.collect();
        if rows.len() < 1 {
            assert!(false, "No rows found: {:?}", rows);
        }
        if rows.len() > 1 {
            assert!(false, "More than one row found: {:?}", rows);
        }
        let rows: Result<Vec<Document>, mongodb::error::Error> = rows.into_iter().collect();

        if let Err(r) = rows {
            return assert!(false, "Error: {:?}", r);
        }

        let row = &rows.unwrap()[0];
        if let Some(f) = (&$value as &dyn Any).downcast_ref::<i32>() {
            let value = row.get_i32($field).unwrap();
            assert_eq!(f, &value);
        } else {
            unimplemented!("can't handle type for {:?}", &$value);
        }
    }};
}
