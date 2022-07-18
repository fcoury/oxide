use bson::doc;
use mongodb::IndexModel;

mod common;

#[test]
fn create_indexes_test() {
    let ctx = common::setup();

    let model = IndexModel::builder().keys(doc! { "x": 1, "z": 1 }).build();
    let options = None;
    ctx.col().create_index(model, options).unwrap();

    let mut cursor = ctx.col().list_indexes(None).unwrap();
    let index = cursor.next().unwrap().unwrap();
    assert_eq!(index.keys, doc! { "x": 1, "z": 1 });
}
