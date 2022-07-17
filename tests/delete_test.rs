use bson::doc;

mod common;

#[test]
fn test_delete_basic() {
    let ctx = common::setup();

    ctx.col()
        .insert_many(
            vec![doc! { "x": 1 }, doc! { "x": 2, "a": 1 }, doc! { "x": 3 }],
            None,
        )
        .unwrap();

    ctx.col().delete_many(doc! { "a": 1 }, None).unwrap();
    let cursor = ctx.col().find(doc! {}, None).unwrap();
    let results = cursor.collect::<Vec<_>>();
    assert_eq!(results.len(), 2);
}

#[test]
fn test_delete_one() {
    // FIXME right now it only crashes
    let ctx = common::setup();

    ctx.col()
        .insert_many(
            vec![doc! { "a": 1 }, doc! { "x": 2, "a": 1 }, doc! { "a": 1 }],
            None,
        )
        .unwrap();

    ctx.col().delete_one(doc! { "a": 1 }, None).unwrap();
    let cursor = ctx.col().find(doc! {}, None).unwrap();
    let results = cursor.collect::<Vec<_>>();
    assert_eq!(results.len(), 2);
}
