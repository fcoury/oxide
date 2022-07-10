#![allow(dead_code)]
use mongodb::bson::Document;
use oxide::pg::PgDb;
use oxide::server::Server;
use std::{env, thread};

pub struct TestContext {
    pub db: String,
    pub collection: String,
    mongodb: mongodb::sync::Client,
}

impl TestContext {
    pub fn new(port: u16, db: String) -> Self {
        let collection = "test_collection".to_string();
        let client_uri = format!("mongodb://localhost:{}/test", port);
        let mongodb = mongodb::sync::Client::with_uri_str(&client_uri).unwrap();

        TestContext {
            db,
            collection,
            mongodb,
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
}

pub fn setup_with_db(name: &str) -> TestContext {
    // static ID_COUNTER: AtomicU32 = AtomicU32::new(0);
    // let id = ID_COUNTER.fetch_add(1, Ordering::Relaxed);

    let _ = env_logger::builder().is_test(true).try_init();
    dotenv::dotenv().ok();

    env::set_var("DATABASE_URL", env::var("TEST_DATABASE_URL").unwrap());

    let port: u16 = portpicker::pick_unused_port().unwrap();
    thread::spawn(move || {
        Server::new("localhost".to_string(), port).start();
    });

    PgDb::new().drop_schema(&name).unwrap();
    TestContext::new(port, name.to_string())
}

pub fn setup() -> TestContext {
    setup_with_db("test_db")
}
