#![allow(dead_code)]
use mongodb::bson::Document;
use mongodb::sync::Cursor;
use oxide::pg::PgDb;
use oxide::server::Server;
use r2d2::Pool;
use r2d2_postgres::{postgres::NoTls, PostgresConnectionManager};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::sync::Once;
use std::{env, thread};

static INIT: Once = Once::new();

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
        let mut buffer = [0u8; 1024];
        stream.read(&mut buffer).unwrap();
        stream.shutdown(Shutdown::Write).unwrap();

        buffer[..].to_vec()
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

pub fn initialize(pool: Pool<PostgresConnectionManager<NoTls>>) {
    INIT.call_once(|| {
        let mut pg = PgDb::new_from_pool(pool);
        pg.exec(
            indoc::indoc! {"
                DO $$ DECLARE
                    r RECORD;
                BEGIN
                    FOR r IN (SELECT indexname FROM pg_indexes WHERE schemaname = 'db_test') LOOP
                        EXECUTE 'DROP INDEX IF EXISTS db_test.' || quote_ident(r.indexname) || ' CASCADE';
                    END LOOP;

                    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'db_test') LOOP
                        EXECUTE 'DROP TABLE IF EXISTS db_test.' || quote_ident(r.tablename) || ' CASCADE';
                    END LOOP;
                END $$;
            "},
            &[],
        )
        .unwrap();
    });
}

pub fn setup_with_pg_db(name: &str, drop: bool) -> TestContext {
    // static ID_COUNTER: AtomicU32 = AtomicU32::new(0);
    // let id = ID_COUNTER.fetch_add(1, Ordering::Relaxed);

    let _ = env_logger::builder().is_test(true).try_init();
    dotenv::dotenv().ok();

    if drop {
        PgDb::new().drop_db(name).unwrap();
    }

    PgDb::new().create_db_if_not_exists(name).unwrap();

    let pg_url = format!("{}/{}", env::var("TEST_DATABASE_URL").unwrap(), name);
    let port: u16 = portpicker::pick_unused_port().unwrap();

    let manager = PostgresConnectionManager::new(pg_url.parse().unwrap(), NoTls);
    let pool = r2d2::Pool::builder().max_size(2).build(manager).unwrap();
    initialize(pool.clone());

    thread::spawn(move || {
        Server::new("localhost".to_string(), port).start_with_pool(pool);
    });

    TestContext::new(port, "db_test".to_string())
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

#[macro_export]
macro_rules! insert {
    ( $( $x:expr ),+ $(,)? ) => {
        {
            let ctx = common::setup();
            ctx.col()
                .insert_many(
                    vec![
                        $( $x, )*
                    ],
                    None,
                )
                .unwrap();
            ctx.col()
        }
    };
}

#[macro_export]
macro_rules! assert_row_count {
    ( $col:expr, $query:expr, $exp:expr ) => {{
        let cursor = $col.find($query, None).unwrap();
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
