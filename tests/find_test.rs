use bson::Document;
use mongodb::bson::doc;

mod common;

#[test]
fn test_basic_find() {
    let ctx = common::setup();

    ctx.col()
        .insert_many(
            vec![doc! { "x": 1 }, doc! { "x": 2, 'a': 1 }, doc! { "x": 3 }],
            None,
        )
        .unwrap();

    let mut cursor = ctx.col().find(doc! { 'a': 1 }, None).unwrap();
    let row1 = cursor.next().unwrap().unwrap();
    assert_eq!(row1.get_i32("x").unwrap(), 2);
    assert!(cursor.next().is_none());
}

#[test]
fn test_find_string() {
    let ctx = common::setup();

    ctx.col()
        .insert_many(
            vec![
                doc! { "x": 1, "name": "Felipe" },
                doc! { "x": 2, "name": "James" },
            ],
            None,
        )
        .unwrap();

    let mut cursor = ctx.col().find(doc! { "name": "James" }, None).unwrap();
    let row1 = cursor.next().unwrap().unwrap();
    assert_eq!(row1.get_i32("x").unwrap(), 2);
    assert!(cursor.next().is_none());
}

#[test]
fn test_find_with_or() {
    let ctx = common::setup();

    ctx.col()
        .insert_many(
            vec![
                doc! { "x": 1, "name": "Peter" },
                doc! { "x": 2, "name": "James" },
                doc! { "x": 3, "name": "Mary" },
            ],
            None,
        )
        .unwrap();

    let cursor = ctx
        .col()
        .find(
            doc! { "$or": vec![ doc!{ "name": "Peter" }, doc! { "x": 3 }] },
            None,
        )
        .unwrap();
    let rows: Vec<Result<Document, mongodb::error::Error>> = cursor.collect();
    assert_eq!(2, rows.len());
    assert_eq!(
        vec!["Peter", "Mary"],
        rows.into_iter()
            .filter(|r| r.is_ok())
            .map(|r| r.unwrap().get_str("name").unwrap().to_string())
            .collect::<Vec<String>>()
    );
}
