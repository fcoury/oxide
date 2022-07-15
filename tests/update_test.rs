use mongodb::bson::doc;

mod common;

#[test]
fn test_basic_update() {
    let ctx = common::setup();

    ctx.col()
        .insert_many(vec![doc! { "x": 1 }, doc! { "x": 2 }], None)
        .unwrap();

    let res = ctx
        .col()
        .update_many(
            doc! { "x": { "$gt": 1 } },
            doc! { "$set": { "x": 3 } },
            None,
        )
        .unwrap();
    println!("{:?}", res);

    let mut cursor = ctx.col().find(doc! { "x": { "$gt": 1 }}, None).unwrap();
    let row1 = cursor.next().unwrap().unwrap();
    assert_eq!(row1.get_i32("x").unwrap(), 3);
    assert!(cursor.next().is_none());
}

#[test]
fn test_update_with_nested_keys() {
    let ctx = common::setup();

    ctx.col()
        .insert_many(vec![doc! { "x": { "y": { "z": 21 } } }], None)
        .unwrap();

    let res = ctx
        .col()
        .update_many(doc! {}, doc! { "$set": { "x.y": 3 } }, None)
        .unwrap();

    let mut cursor = ctx.col().find(doc! {}, None).unwrap();
    let row1 = cursor.next().unwrap().unwrap();
    assert_eq!(row1.get_document("x").unwrap(), &doc! { "y": 3 });
    assert_eq!(res.matched_count, 1);
    assert_eq!(res.modified_count, 1);
}

#[test]
fn test_update_with_conflicting_nested_keys() {
    let ctx = common::setup();

    ctx.col()
        .insert_many(vec![doc! { "field": { "y": { "z": 21 } } }], None)
        .unwrap();

    let res = ctx.col().update_many(
        doc! {},
        doc! { "$set": { "field.y.z": 3, "field.z": 2 } },
        None,
    );

    assert!(res.is_err());
    assert!(res
        .unwrap_err()
        .to_string()
        .contains("Cannot update 'field.y.z' and 'field.z' at the same time"));
}

#[test]
fn test_update_with_conflicting_keys() {
    let ctx = common::setup();

    ctx.col()
        .insert_many(vec![doc! { "field": { "y": { "z": 21 } } }], None)
        .unwrap();

    let res = ctx.col().update_many(
        doc! {},
        doc! { "$set": { "field": { "y": { "z": 3 } }, "field.z": 2 } },
        None,
    );

    assert!(res.is_err());
    assert!(res
        .unwrap_err()
        .to_string()
        .contains("Cannot update 'field' and 'field.z' at the same time"));
}
