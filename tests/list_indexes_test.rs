use bson::doc;

mod common;

#[test]
fn test_list_indexes_basic() {
    let ctx = common::setup();

    ctx.col().insert_one(doc! { "x": 1 }).run().unwrap();

    let res = ctx
        .db()
        .run_command(doc! {
            "listIndexes": ctx.clone().collection,
        })
        .run()
        .unwrap();

    let cursor = res.get_document("cursor").unwrap();
    let indexes = cursor.get_array("firstBatch").unwrap();
    assert_eq!(indexes.len(), 1);
    assert_eq!(
        indexes[0].as_document().unwrap().get_str("name").unwrap(),
        "_id_"
    );
    assert_eq!(cursor.get_i64("id").unwrap(), 0);
    assert_eq!(
        cursor.get_str("ns").unwrap(),
        format!("{}.$cmd.listIndexes.{}", ctx.db, ctx.collection)
    );
}

#[test]
fn test_list_indexes_collection_not_found() {
    let ctx = common::setup();

    let res = ctx
        .db()
        .run_command(doc! {
            "listIndexes": ctx.clone().collection,
        })
        .run();

    assert!(res.is_err());
    assert!(res.unwrap_err().to_string().contains("NamespaceNotFound"));
}

#[test]
fn list_indexes_honors_cursor_options_and_get_more() {
    let ctx = common::setup();
    ctx.col().insert_one(doc! {"x": 1}).run().unwrap();
    let first = ctx
        .db()
        .run_command(doc! {
            "listIndexes": &ctx.collection, "cursor": {"batchSize": 0}
        })
        .run()
        .unwrap();
    let cursor = first.get_document("cursor").unwrap();
    assert!(cursor.get_array("firstBatch").unwrap().is_empty());
    let id = cursor.get_i64("id").unwrap();
    assert_ne!(id, 0);
    let next = ctx.db().run_command(doc! {
        "getMore": id, "collection": format!("$cmd.listIndexes.{}", ctx.collection), "batchSize": 1
    }).run().unwrap();
    let cursor = next.get_document("cursor").unwrap();
    assert_eq!(cursor.get_i64("id").unwrap(), 0);
    assert_eq!(cursor.get_array("nextBatch").unwrap().len(), 1);
    for options in [doc! {"batchSize": -1}, doc! {"unknown": 1}] {
        assert!(ctx
            .db()
            .run_command(doc! {"listIndexes": &ctx.collection, "cursor": options})
            .run()
            .is_err());
    }
}
