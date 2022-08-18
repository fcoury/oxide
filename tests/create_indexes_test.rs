use bson::doc;
use mongodb::IndexModel;

mod common;

#[test]
fn create_indexes_test() {
    let ctx = common::setup();

    let model = IndexModel::builder().keys(doc! { "x": 1, "z": 1 }).build();
    let options = None;
    ctx.col().create_index(model, options).unwrap();

    let cursor = ctx.col().list_indexes(None).unwrap();
    let indexes = cursor
        .collect::<Vec<_>>()
        .iter()
        .map(|x| x.clone().unwrap())
        .collect::<Vec<_>>();
    let index = indexes
        .iter()
        .find(|x| x.keys == doc! { "x": 1, "z": 1 })
        .unwrap();
    assert_eq!(index.keys, doc! { "x": 1, "z": 1 });
}

#[test]
fn create_indexes_test_already_existing() {
    let ctx = common::setup();

    let model = IndexModel::builder().keys(doc! { "a": 1, "b": 1 }).build();
    let options = None;
    ctx.col()
        .create_index(model.clone(), options.clone())
        .unwrap();
    ctx.col().create_index(model, options).unwrap();

    let cursor = ctx.col().list_indexes(None).unwrap();
    let indexes = cursor
        .collect::<Vec<_>>()
        .iter()
        .map(|x| x.clone().unwrap())
        .collect::<Vec<_>>();
    let index = indexes
        .iter()
        .find(|x| x.keys == doc! { "a": 1, "b": 1 })
        .unwrap();
    assert_eq!(index.keys, doc! { "a": 1, "b": 1 });
}
