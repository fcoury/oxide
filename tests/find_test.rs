use bson::{Bson, Document};
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

#[test]
fn test_find_with_float() {
    let ctx = common::setup();

    ctx.col()
        .insert_many(
            vec![
                doc! { "x": 1.2, "name": "Peter" },
                doc! { "x": 2.3, "name": "James" },
                doc! { "x": 3, "name": "Mary" },
            ],
            None,
        )
        .unwrap();

    let cursor = ctx
        .col()
        .find(doc! { "x": Bson::Double(2.3) }, None)
        .unwrap();
    let rows: Vec<Result<Document, mongodb::error::Error>> = cursor.collect();
    assert_eq!(1, rows.len());
    assert_eq!(
        "James",
        rows.into_iter()
            .next()
            .unwrap()
            .unwrap()
            .get_str("name")
            .unwrap()
    );
}

#[test]
fn test_find_with_gt_float() {
    let ctx = common::setup();

    ctx.col()
        .insert_many(
            vec![
                doc! { "x": 1.2, "name": "Peter" },
                doc! { "x": 2.3, "name": "James" },
                doc! { "x": 3, "name": "Mary" },
            ],
            None,
        )
        .unwrap();

    let cursor = ctx.col().find(doc! { "x": { "$lte": 2.3 } }, None).unwrap();
    let rows: Vec<Result<Document, mongodb::error::Error>> = cursor.collect();
    assert_eq!(2, rows.len());
    assert_eq!(
        "Peter",
        rows.into_iter()
            .next()
            .unwrap()
            .unwrap()
            .get_str("name")
            .unwrap()
    );
}

#[test]
fn test_find_type_bracketing() {
    let ctx = common::setup();

    ctx.col()
        .insert_many(
            vec![
                doc! { "counter": 1 },
                doc! { "counter": "Str" },
                doc! { "counter": 3 },
            ],
            None,
        )
        .unwrap();

    let cursor = ctx
        .col()
        .find(doc! { "counter": { "$gt": 1 } }, None)
        .unwrap();
    let rows: Vec<Result<Document, mongodb::error::Error>> = cursor.collect();
    assert_eq!(1, rows.len());
}

#[test]
fn test_find_with_exists() {
    let ctx = common::setup();

    ctx.col()
        .insert_many(
            vec![
                doc! { "counter": 1, "a": 1 },
                doc! { "counter": "Str", "a": { "b": false } },
                doc! { "counter": 3, "d": 0 },
            ],
            None,
        )
        .unwrap();

    let res = ctx
        .col()
        .find(doc! { "a": { "$exists": true } }, None)
        .unwrap()
        .map(|r| r.unwrap())
        .collect::<Vec<_>>();
    assert_eq!(2, res.len());

    let res = ctx
        .col()
        .find(doc! { "a.b": { "$exists": true } }, None)
        .unwrap()
        .map(|r| r.unwrap())
        .collect::<Vec<_>>();
    assert_eq!(1, res.len());
    assert_eq!("Str", res[0].get_str("counter").unwrap());

    let res = ctx
        .col()
        .find(doc! { "a.b": { "$exists": false } }, None)
        .unwrap()
        .map(|r| r.unwrap().get("counter").unwrap().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(2, res.len());
    assert_eq!(res, [Bson::Int32(1), Bson::Int32(3)]);

    let res = ctx
        .col()
        .find(doc! { "a": { "b": { "$exists": false } } }, None)
        .unwrap()
        .map(|r| r.unwrap().get("counter").unwrap().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(2, res.len());
    assert_eq!(res, [Bson::Int32(1), Bson::Int32(3)]);
}

#[test]
fn find_with_in() {
    let ctx = common::setup();

    ctx.col()
        .insert_many(
            vec![
                doc! { "counter": 1, "a": 1 },
                doc! { "counter": "Str", "a": { "b": false } },
                doc! { "counter": 3, "a": 2 },
            ],
            None,
        )
        .unwrap();

    let res = ctx
        .col()
        .find(doc! { "a": { "$in": [1, 2] } }, None)
        .unwrap()
        .map(|r| r.unwrap())
        .collect::<Vec<_>>();
    assert_eq!(2, res.len());

    let res = ctx
        .col()
        .find(doc! { "a": { "$nin": [1, 2] } }, None)
        .unwrap()
        .map(|r| r.unwrap())
        .collect::<Vec<_>>();
    assert_eq!(1, res.len());
}

#[test]
fn test_with_nested() {
    let col = insert! {
        doc! { "a": { "b": { "c": 1 } } }
    };

    assert_row_count!(col, doc! { "a.b.c": 1 }, 1);
}

#[test]
fn test_with_multiple_fields() {
    let col = insert!(
        doc! { "counter": 1, "a": 1 },
        doc! { "counter": "Str", "a": { "b": false } },
        doc! { "counter": 3, "d": 0 },
    );

    let cursor = col.find(doc! { "counter": 1, "a": 1 }, None).unwrap();
    let rows: Vec<Result<Document, mongodb::error::Error>> = cursor.collect();
    let rows: Result<Vec<Document>, mongodb::error::Error> = rows.into_iter().collect();
    let rows: Vec<Document> = rows.unwrap();
    assert_eq!(1, rows[0].get_i32("counter").unwrap());
    assert_eq!(1, rows[0].get_i32("a").unwrap());
}
