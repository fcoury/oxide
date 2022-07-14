use mongodb::bson::{doc, Document};

mod common;

#[test]
fn test_drop_database() {
    let ctx = common::setup(); // setup_with_pg_db("test_drop_database_1");
    let db = ctx.mongodb().database("test_drop_database_1");
    let col = db.collection::<Document>("test");

    col.insert_one(doc! { "x": 1 }, None).unwrap();

    let dbs = ctx.mongodb().list_database_names(None, None).unwrap();
    println!("{:?}", dbs);
    assert!(dbs.contains(&"test_drop_database_1".to_string()));

    db.drop(None).unwrap();

    let dbs = ctx.mongodb().list_database_names(None, None).unwrap();
    assert!(!dbs.contains(&"test_drop_database_1".to_string()));
}

#[test]
fn test_drop_inexistent_database() {
    let ctx = common::setup(); // setup_with_pg_db("test_drop_database_1");
    let db = ctx.mongodb().database("test_drop_database_1");

    let dbs = ctx.mongodb().list_database_names(None, None).unwrap();
    println!("{:?}", dbs);
    assert!(!dbs.contains(&"test_drop_database_1".to_string()));

    db.drop(None).unwrap();

    let dbs = ctx.mongodb().list_database_names(None, None).unwrap();
    assert!(!dbs.contains(&"test_drop_database_1".to_string()));
}
