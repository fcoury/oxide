use mongodb::bson::doc;

mod common;

#[test]
fn test_list_database() {
    let ctx = common::setup_with_pg_db("test_list_1");

    // initially only public database is listed
    let res = ctx.mongodb().list_databases(None, None).unwrap();
    assert_eq!(res.len(), 1);
    assert_eq!(res.get(0).unwrap().name, "public");

    ctx.col().insert_one(doc! { "x": 1 }, None).unwrap();

    // lists the newly created database
    let res = ctx.mongodb().list_databases(None, None).unwrap();
    assert_eq!(res.len(), 2);
    let dbs = res.iter().map(|db| db.name.to_owned()).collect::<Vec<_>>();
    assert!(dbs.contains(&"public".to_string()));
    assert!(dbs.contains(&ctx.db));
}

#[test]
fn test_list_database_name_only() {
    let ctx = common::setup_with_pg_db("test_list_2");

    let res = ctx.mongodb().list_database_names(None, None).unwrap();
    assert_eq!(res.len(), 1);
    assert_eq!(res.get(0).unwrap(), "public");
}

#[test]
fn test_list_database_with_table_with_spaces() {
    let ctx = common::setup_with_pg_db("test_list_3");

    ctx.db()
        .collection("my col")
        .insert_one(doc! { "x": 1 }, None)
        .unwrap();

    let res = ctx.mongodb().list_databases(None, None).unwrap();
    assert_eq!(res.len(), 2);
}
