#![cfg(feature = "shell")]
use bson::{doc, Bson};
use mongodb::{
    options::{ClientOptions, ServerAddress},
    sync::Client,
};
use oxide::shell::Shell;
use std::time::Duration;
mod common;

fn options(port: u16) -> ClientOptions {
    ClientOptions::builder()
        .hosts(vec![ServerAddress::Tcp {
            host: "127.0.0.1".into(),
            port: Some(port),
        }])
        .server_selection_timeout(Duration::from_secs(5))
        .direct_connection(true)
        .retry_writes(false)
        .retry_reads(false)
        .build()
}

fn exercise(options: ClientOptions, database: &str) {
    let client = Client::with_options(options.clone()).unwrap();
    let mut shell = Shell::new(options).unwrap();
    shell.eval(&format!("use {database}")).unwrap();
    let result = shell
        .eval(include_str!("fixtures/shell.js"))
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&result).unwrap(),
        serde_json::json!({"ok":true,"remaining":247})
    );
    assert!(shell
        .eval("show collections")
        .unwrap()
        .unwrap()
        .contains("shell_items"));
    assert!(shell
        .eval("show databases")
        .unwrap()
        .unwrap()
        .contains(database));

    // Typed fields survive a read/modify/write through JavaScript, including
    // native BSON values with no JavaScript counterpart.
    let col = client
        .database(database)
        .collection::<bson::Document>("typed");
    col.insert_one(doc! {"_id":1, "long":i64::MAX, "double":1.0, "zero":-0.0, "distant":bson::DateTime::from_millis(i64::MIN), "timestamp":bson::Timestamp{time:1,increment:2}, "min":Bson::MinKey}).run().unwrap();
    shell
        .eval("let typed = db.typed.findOne({_id:1}); typed._id = 2; db.typed.insertOne(typed)")
        .unwrap();
    let original = col.find_one(doc! {"_id":1}).run().unwrap().unwrap();
    let copied = col.find_one(doc! {"_id":2}).run().unwrap().unwrap();
    for field in ["long", "double", "zero", "distant", "timestamp", "min"] {
        assert_eq!(original.get(field), copied.get(field), "{field}");
    }
    assert!(copied.get_f64("zero").unwrap().is_sign_negative());

    // An uncatchable interpreter interruption must also dispose an open cursor.
    let signal = shell.interrupt_handle();
    let sender = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(200));
        signal.send_replace(true);
    });
    assert!(shell.eval("globalThis.openCursor = db.runCommand({find:'shell_items', batchSize:1}).cursor.id; while(true) {}").is_err());
    sender.join().unwrap();
    let id: String =
        serde_json::from_str(&shell.eval("String(openCursor)").unwrap().unwrap()).unwrap();
    let error = client
        .database(database)
        .run_command(doc! {"getMore": id.parse::<i64>().unwrap(), "collection":"shell_items"})
        .run()
        .unwrap_err();
    assert!(
        error.to_string().contains("43") || error.to_string().contains("not found"),
        "{error}"
    );
    assert_eq!(shell.eval("21 * 2").unwrap().as_deref(), Some("42"));
    let error = shell.eval("Promise.resolve().then(() => db.typed.insertOne({_id:'stale-job'})); throw new Error('stop')").unwrap_err();
    assert!(error.to_string().contains("session reset"));
    assert_eq!(shell.database().unwrap(), "test");
    assert_eq!(shell.eval("21 * 2").unwrap().as_deref(), Some("42"));
    assert!(col
        .find_one(doc! {"_id":"stale-job"})
        .run()
        .unwrap()
        .is_none());
    client.database(database).drop().run().unwrap();
}

#[test]
fn owned_shell_works_with_oxide() {
    let ctx = common::setup();
    exercise(
        options(ctx.port()),
        &format!("shell_{}", uuid::Uuid::new_v4().simple()),
    );
}

#[test]
#[ignore = "requires MONGODB_REFERENCE_URL; executed in CI"]
fn owned_shell_works_with_mongodb() {
    let options = ClientOptions::parse(std::env::var("MONGODB_REFERENCE_URL").unwrap())
        .run()
        .unwrap();
    exercise(
        options,
        &format!("oxide_shell_{}", uuid::Uuid::new_v4().simple()),
    );
}

#[test]
#[ignore = "requires MONGODB_REFERENCE_URL; executed in CI"]
fn shell_bounds_results_and_recovers() {
    let options = ClientOptions::parse(std::env::var("MONGODB_REFERENCE_URL").unwrap())
        .run()
        .unwrap();
    let client = Client::with_options(options.clone()).unwrap();
    let db = client.database(&format!(
        "oxide_shell_limits_{}",
        uuid::Uuid::new_v4().simple()
    ));
    let col = db.collection::<bson::Document>("items");
    let mut shell = Shell::new(options).unwrap();
    shell.eval(&format!("use {}", db.name())).unwrap();
    col.insert_many((0..10001).map(|i| doc! {"_id":i}))
        .run()
        .unwrap();
    let error = shell
        .eval("db.items.find({}, {batchSize:100})")
        .unwrap_err()
        .to_string();
    assert!(error.contains("10000 documents"), "{error}");
    assert_eq!(
        shell
            .eval("db.items.find({}, {limit:1}).length")
            .unwrap()
            .as_deref(),
        Some("1")
    );
    col.drop().run().unwrap();
    col.insert_many((0..30).map(|i| doc! {"_id":i, "text":"x".repeat(300_000)}))
        .run()
        .unwrap();
    let error = shell
        .eval("db.items.find({}, {batchSize:1})")
        .unwrap_err()
        .to_string();
    assert!(error.contains("8 MiB"), "{error}");
    assert_eq!(
        shell
            .eval("db.items.find({}, {limit:1}).length")
            .unwrap()
            .as_deref(),
        Some("1")
    );
    db.drop().run().unwrap();
}
