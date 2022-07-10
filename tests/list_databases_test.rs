#![allow(dead_code)]
use mongodb::bson::{doc, Document};
use oxide::pg::PgDb;
use oxide::server::Server;
use std::{
    env,
    thread::{self, JoinHandle},
};

struct TestContext {
    pub db: String,
    pub collection: String,
    mongodb: mongodb::sync::Client,
    handle: JoinHandle<()>,
}

impl TestContext {
    pub fn new(handle: JoinHandle<()>, port: u16, db: String) -> Self {
        let collection = "test_collection".to_string();

        let client_uri = format!("mongodb://localhost:{}/test", port);
        let mongodb = mongodb::sync::Client::with_uri_str(&client_uri).unwrap();

        TestContext {
            db,
            collection,
            mongodb,
            handle,
        }
    }

    pub fn mongodb(&self) -> &mongodb::sync::Client {
        &self.mongodb
    }

    pub fn pg(self) -> PgDb {
        PgDb::new()
    }

    pub fn col(&self) -> mongodb::sync::Collection<Document> {
        self.mongodb
            .database(self.db.as_str())
            .collection(self.collection.as_str())
    }
}

fn setup() -> TestContext {
    let _ = env_logger::builder().is_test(true).try_init();
    dotenv::dotenv().ok();

    env::set_var("DATABASE_URL", env::var("TEST_DATABASE_URL").unwrap());

    let port: u16 = portpicker::pick_unused_port().unwrap();
    let handle = thread::spawn(move || {
        Server::new("localhost".to_string(), port).start();
    });

    PgDb::new().drop_schema("test_db").unwrap();

    TestContext::new(handle, port, "test_db".to_string())
}

#[test]
fn test_list_database() {
    let ctx = setup();

    // initially only public database is listed
    let res = ctx.mongodb().list_databases(None, None).unwrap();
    assert_eq!(res.len(), 1);
    assert_eq!(res.get(0).unwrap().name, "public");

    ctx.col().insert_one(doc! { "x": 1 }, None).unwrap();

    // lists the newly created database
    let res = ctx.mongodb().list_databases(None, None).unwrap();
    assert_eq!(res.len(), 2);
    assert!(res.get(1).unwrap().name == "test_db");
}
