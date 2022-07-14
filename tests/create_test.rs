use bson::{doc, Document};

mod common;

#[test]
fn test_create_basic() {
    let ctx = common::setup();

    ctx.db()
        .collection::<Document>(&ctx.collection)
        .drop(None)
        .unwrap();

    let names = ctx.db().list_collection_names(None).unwrap();
    assert!(!names.contains(&ctx.collection));

    ctx.db()
        .run_command(
            doc! {
                "create": ctx.clone().collection,
            },
            None,
        )
        .unwrap();

    let names = ctx.db().list_collection_names(None).unwrap();
    assert!(names.contains(&ctx.collection));
}
