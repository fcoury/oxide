use bson::doc;

mod common;

#[test]
fn test_list_indexes_basic() {
    let ctx = common::setup();

    ctx.col().insert_one(doc! { "x": 1 }, None).unwrap();

    let res = ctx
        .db()
        .run_command(
            doc! {
                "listIndexes": ctx.clone().collection,
            },
            None,
        )
        .unwrap();

    let cursor = res.get_document("cursor").unwrap();
    assert_eq!(cursor.get_array("firstBatch").unwrap(), &vec![]);
    assert_eq!(cursor.get_i64("id").unwrap(), 0);
    assert_eq!(
        cursor.get_str("ns").unwrap(),
        format!("{}.$cmd.listIndexes.{}", ctx.db, ctx.collection)
    );
}

#[test]
fn test_list_indexes_collection_not_found() {
    let ctx = common::setup();

    let res = ctx.db().run_command(
        doc! {
            "listIndexes": ctx.clone().collection,
        },
        None,
    );

    assert!(res.is_err());
    assert_eq!(
        res.unwrap_err().to_string(),
        format!(
            r#"Command failed (CommandNotFound): Collection '{}' doesn't exist)"#,
            ctx.collection
        )
    );
}
