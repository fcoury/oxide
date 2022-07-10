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

    pub fn col(&self) -> mongodb::sync::Collection<Document> {
        self.mongodb()
            .database(self.db.as_str())
            .collection(self.collection.as_str())
    }
}

pub fn setup() -> TestContext {
    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Debug)
        .try_init();
    dotenv::dotenv().ok();

    env::set_var("DATABASE_URL", env::var("TEST_DATABASE_URL").unwrap());

    let port: u16 = portpicker::pick_unused_port().unwrap();
    thread::spawn(move || {
        Server::new("localhost".to_string(), port).start();
    });

    PgDb::new().drop_schema("test_db").unwrap();

    TestContext::new(port, "test_db".to_string())
}
