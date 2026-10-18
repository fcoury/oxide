use bson::{doc, Document};

mod common;

#[test]
fn test_create_basic() {
    let ctx = common::setup();

    ctx.db()
        .collection::<Document>(&ctx.collection)
        .drop()
        .run()
        .unwrap();

    let names = ctx.db().list_collection_names().run().unwrap();
    assert!(!names.contains(&ctx.collection));

    ctx.db()
        .run_command(doc! {
            "create": ctx.clone().collection,
        })
        .run()
        .unwrap();

    let names = ctx.db().list_collection_names().run().unwrap();
    assert!(names.contains(&ctx.collection));
}
