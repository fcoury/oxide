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

#[test]
fn test_update_with_inc() {
    let ctx = common::setup();

    ctx.col()
        .insert_many(
            vec![
                doc! { "one": 1, "v": 1 },
                doc! { "two": 2, "v": 2 },
                doc! { "three": "three", "v": 0 },
            ],
            None,
        )
        .unwrap();

    ctx.col()
        .update_many(doc! { "one": 1 }, doc! { "$inc": { "v": 1 } }, None)
        .unwrap();

    let mut res = ctx.col().find(doc! { "one": 1 }, None).unwrap();
    let row1 = res.next().unwrap().unwrap();
    assert_eq!(row1.get_i32("v").unwrap(), 2);
}

#[test]
fn test_update_with_inc_multiple_fields() {
    let ctx = common::setup();

    ctx.col()
        .insert_many(
            vec![
                doc! { "one": 1, "v": 1, "z": 1 },
                doc! { "one": 1, "v": 2, "z": 10 },
                doc! { "three": "three", "v": 0, "z": 100 },
            ],
            None,
        )
        .unwrap();

    ctx.col()
        .update_many(
            doc! { "one": 1 },
            doc! { "$inc": { "v": 10, "z": -2 } },
            None,
        )
        .unwrap();

    let mut res = ctx.col().find(doc! { "one": 1 }, None).unwrap();
    let row1 = res.next().unwrap().unwrap();
    assert_eq!(row1.get_i32("v").unwrap(), 11);
    assert_eq!(row1.get_i32("z").unwrap(), -1);
    let row2 = res.next().unwrap().unwrap();
    assert_eq!(row2.get_i32("v").unwrap(), 12);
    assert_eq!(row2.get_i32("z").unwrap(), 8);
}

#[test]
#[ignore]
fn test_update_with_inc_double_fields() {
    // FIXME We have to change how updates work, instead of doing
    //       them in batch, we'll have to get one doc, update it
    //       and so on.
    // #16 - https://github.com/fcoury/oxide/issues/16
    let ctx = common::setup();

    ctx.col()
        .insert_many(
            vec![
                doc! { "one": 1, "v": 1, "z": 1.5 },
                doc! { "one": 1, "v": 2, "z": 10.2 },
                doc! { "three": "three", "v": 0, "z": 100 },
            ],
            None,
        )
        .unwrap();

    ctx.col()
        .update_many(
            doc! { "one": 1 },
            doc! { "$inc": { "v": 10, "z": -2 } },
            None,
        )
        .unwrap();

    let mut res = ctx.col().find(doc! { "one": 1 }, None).unwrap();
    let row1 = res.next().unwrap().unwrap();
    assert_eq!(row1.get_i32("v").unwrap(), 11);
    assert_eq!(row1.get_f64("z").unwrap(), -0.5);
    let row2 = res.next().unwrap().unwrap();
    assert_eq!(row2.get_i32("v").unwrap(), 12);
    assert_eq!(row2.get_f64("z").unwrap(), 8.2);
    */
}

#[test]
#[ignore]
fn test_update_inc_with_nested_fields() {
    todo!("nested fields are exanded but we don't consider those when building the update clause");
}
