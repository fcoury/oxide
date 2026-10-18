use mongodb::bson::doc;

mod common;

#[test]
fn test_list_database() {
    let ctx = common::setup_with_pg_db("test_list_1", true);

    // Unowned PostgreSQL schemas are not exposed as MongoDB databases.
    let res = ctx.mongodb().list_databases().run().unwrap();
    assert!(res.is_empty());

    ctx.col().insert_one(doc! { "x": 1 }).run().unwrap();

    // lists the newly created database
    let res = ctx.mongodb().list_databases().run().unwrap();
    assert_eq!(res.len(), 1);
    let dbs = res.iter().map(|db| db.name.to_owned()).collect::<Vec<_>>();
    assert!(dbs.contains(&ctx.db));
}

#[test]
fn test_list_database_name_only() {
    let ctx = common::setup_with_pg_db("test_list_2", true);

    let res = ctx.mongodb().list_database_names().run().unwrap();
    assert!(res.is_empty());
}

#[test]
fn test_list_database_with_table_with_spaces() {
    let ctx = common::setup_with_pg_db("test_list_3", true);

    ctx.db()
        .collection("my col")
        .insert_one(doc! { "x": 1 })
        .run()
        .unwrap();

    let res = ctx.mongodb().list_databases().run().unwrap();
    assert_eq!(res.len(), 1);
}
