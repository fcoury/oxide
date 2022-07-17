use mongodb::bson::doc;

mod common;

#[test]
fn test_basic_update() {
    let ctx = common::setup();

    ctx.col()
        .insert_many(vec![doc! { "x": 1 }, doc! { "x": 2 }], None)
        .unwrap();

    ctx.col()
        .update_many(
            doc! { "x": { "$gt": 1 } },
            doc! { "$set": { "x": 3 } },
            None,
        )
        .unwrap();

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
}

#[test]
#[ignore]
fn test_update_inc_with_nested_fields() {
    todo!("nested fields are exanded but we don't consider those when building the update clause");
}

#[test]
fn test_update_unset() {
    let ctx = common::setup();

    ctx.col()
        .insert_many(
            vec![
                doc! { "idx": 1, "one": 1, "v": 1, "z": 1.5 },
                doc! { "idx": 2, "one": 1, "v": 2, "z": 10.2 },
                doc! { "idx": 3, "three": "three", "v": 0, "z": 100 },
            ],
            None,
        )
        .unwrap();

    ctx.col()
        .update_many(doc! { "one": 1 }, doc! { "$unset": { "v": 1 } }, None)
        .unwrap();

    let mut res = ctx.col().find(doc! { "idx": 1 }, None).unwrap();
    let row1 = res.next().unwrap().unwrap();
    assert_eq!(row1.get("v"), None);
    assert_eq!(row1.get_f64("z").unwrap(), 1.5);

    let mut res = ctx.col().find(doc! { "idx": 2 }, None).unwrap();
    let row2 = res.next().unwrap().unwrap();
    assert_eq!(row2.get("v"), None);
    assert_eq!(row2.get_f64("z").unwrap(), 10.2);

    let mut res = ctx.col().find(doc! { "idx": 3 }, None).unwrap();
    let row3 = res.next().unwrap().unwrap();
    assert_eq!(row3.get_i32("v").unwrap(), 0);
}

#[test]
fn test_update_unset_multiple_fields() {
    let ctx = common::setup();

    ctx.col()
        .insert_many(
            vec![
                doc! { "idx": 1, "one": 1, "v": 1, "z": 1.5 },
                doc! { "idx": 2, "one": 1, "v": 2, "z": 10.2 },
                doc! { "idx": 3, "three": "three", "v": 0, "z": 100 },
            ],
            None,
        )
        .unwrap();

    ctx.col()
        .update_many(
            doc! { "one": 1 },
            doc! { "$unset": { "v": 1, "z": 1 } },
            None,
        )
        .unwrap();

    let mut res = ctx.col().find(doc! { "idx": 1 }, None).unwrap();
    let row1 = res.next().unwrap().unwrap();
    assert_eq!(row1.get("v"), None);
    assert_eq!(row1.get("z"), None);

    let mut res = ctx.col().find(doc! { "idx": 2 }, None).unwrap();
    let row2 = res.next().unwrap().unwrap();
    assert_eq!(row2.get("v"), None);
    assert_eq!(row2.get("z"), None);

    let mut res = ctx.col().find(doc! { "idx": 3 }, None).unwrap();
    let row3 = res.next().unwrap().unwrap();
    assert_eq!(row3.get_i32("v").unwrap(), 0);
    assert_eq!(row3.get_i32("z").unwrap(), 100);
}

#[test]
fn test_update_unset_nested_fields() {
    let ctx = common::setup();

    ctx.col()
        .insert_many(
            vec![
                doc! { "idx": 1, "a": 1, "b": 1, "nested": { "remove": 1, "other": 2, "keep": 1 } },
                doc! { "idx": 2, "a": 1, "b": 1, "nested": { "remove": 1, "other": 2, "keep": 1 } },
                doc! { "idx": 3, "a": 1, "b": 1, "nested": { "remove": 1, "other": 2, "keep": 1 } },
            ],
            None,
        )
        .unwrap();

    ctx.col()
        .update_many(
            doc! { "idx": 1 },
            doc! { "$unset": { "a": 1, "nested": { "remove": 1, "other": 1 }, "b": 1 } },
            None,
        )
        .unwrap();

    let mut res = ctx.col().find(doc! { "idx": 1 }, None).unwrap();
    let row1 = res.next().unwrap().unwrap();

    assert_eq!(row1.get("a"), None);
    assert_eq!(row1.get("b"), None);

    let doc = row1.get_document("nested").unwrap();
    assert_eq!(doc.get("remove"), None);
    assert_eq!(doc.get_i32("keep").unwrap(), 1);
}

#[test]
fn test_update_multi_off() {
    let ctx = common::setup();

    ctx.col()
        .insert_many(
            vec![doc! { "x": 1 }, doc! { "x": 1 }, doc! { "x": 1 }],
            None,
        )
        .unwrap();

    ctx.col()
        .update_one(doc! { "x": 1 }, doc! { "$set": { "x": 10 } }, None)
        .unwrap();

    let cursor = ctx.col().find(doc! { "x": 10 }, None).unwrap();
    let results = cursor.collect::<Vec<_>>();

    assert_eq!(results.len(), 1);
}

#[test]
fn test_update_one_with_replacement_document() {
    let ctx = common::setup();

    ctx.col()
        .insert_many(
            vec![doc! { "x": 1, "y": 2 }, doc! { "x": 2 }, doc! { "x": 3 }],
            None,
        )
        .unwrap();

    ctx.col()
        .replace_one(doc! { "x": 1 }, doc! { "new_key": "oh_yes" }, None)
        .unwrap();

    let cursor = ctx.col().find(doc! { "x": 1 }, None).unwrap();
    let results = cursor.collect::<Vec<_>>();
    assert_eq!(results.len(), 0);

    let cursor = ctx.col().find(doc! { "x": 2 }, None).unwrap();
    let results = cursor.collect::<Vec<_>>();
    assert_eq!(results.len(), 1);

    let cursor = ctx.col().find(doc! { "new_key": "oh_yes" }, None).unwrap();
    let results = cursor.collect::<Vec<_>>();
    assert_eq!(results[0].clone().unwrap(), doc! { "new_key": "oh_yes" });
}

#[test]
fn test_large_update() {
    let ctx = common::setup();
    ctx.send_file("tests/fixtures/binaries/large-update-1.bin");
}
