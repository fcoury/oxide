use bson::Bson;
use chrono::Utc;
use mongodb::{bson::doc, options::ReplaceOptions};

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
    todo!("nested fields are expanded but we don't consider those when building the update clause");
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
    assert_eq!(
        results[0].clone().unwrap().get_str("new_key").unwrap(),
        "oh_yes"
    );
}

#[test]
fn test_upsert() {
    let ctx = common::setup();
    let res = ctx
        .col()
        .replace_one(
            doc! { "x": 1 },
            doc! { "x": 1, "y": 2 },
            ReplaceOptions::builder().upsert(true).build(),
        )
        .unwrap();
    let count = res.modified_count;
    assert_eq!(count, 1);

    let cursor = ctx.col().find(doc! { "x": 1 }, None).unwrap();
    let results = cursor.collect::<Vec<_>>();
    assert_eq!(results.len(), 1);
}

#[test]
fn test_upsert_without_id() {
    let ctx = common::setup();
    ctx.db()
        .run_command(
            doc! {
                "update": &ctx.collection,
                "updates": vec![doc! {
                    "q": {},
                    "u": {
                        "name": "Felipe"
                    },
                    "upsert": true,
                }],
            },
            None,
        )
        .unwrap();
    let doc = ctx
        .col()
        .find(doc! { "name": "Felipe" }, None)
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert!(doc.contains_key("_id"));
}

#[test]
#[ignore = "failing"]
fn test_large_update() {
    let ctx = common::setup();
    ctx.send_file("tests/fixtures/binaries/large-update-1.bin");
}

#[test]
fn test_update_with_oid() {
    let ctx = common::setup();

    ctx.col()
        .insert_many(
            vec![doc! { "x": 1 }, doc! { "x": 2, "a": 1 }, doc! { "x": 3 }],
            None,
        )
        .unwrap();
    let oid = ctx
        .col()
        .find_one(doc! {}, None)
        .unwrap()
        .unwrap()
        .get_object_id("_id")
        .unwrap();

    ctx.col()
        .update_one(doc! { "_id": oid }, doc! { "$set": { "x": 10 } }, None)
        .unwrap();
    let res = ctx.col().find(doc! { "_id": oid }, None).unwrap();
    assert_unique_row_value!(res, "x", 10);
}

#[test]
fn test_update_with_date_time() {
    let ctx = common::setup();

    ctx.col()
        .insert_many(
            vec![doc! { "x": 1 }, doc! { "x": 2, "a": 1 }, doc! { "x": 3 }],
            None,
        )
        .unwrap();
    let oid = ctx
        .col()
        .find_one(doc! {}, None)
        .unwrap()
        .unwrap()
        .get_object_id("_id")
        .unwrap();

    let chrono_dt: chrono::DateTime<Utc> = "2014-11-28T12:00:09Z".parse().unwrap();
    let bson_dt: bson::DateTime = chrono_dt.into();
    ctx.col()
        .update_one(
            doc! { "_id": oid },
            doc! { "$set": { "lastModifiedDate": bson_dt } },
            None,
        )
        .unwrap();
    let res = common::get_rows(ctx.col().find(doc! { "_id": oid }, None).unwrap());
    assert_eq!(res[0].get_datetime("lastModifiedDate").unwrap(), &bson_dt);
}

#[test]
fn test_update_with_null() {
    let ctx = common::setup();

    ctx.col()
        .insert_many(
            vec![
                doc! { "enqueued": 1, "notifications": null },
                doc! { "x": 2, "a": 1 },
                doc! { "x": 3 },
            ],
            None,
        )
        .unwrap();
    let oid = ctx
        .col()
        .find_one(doc! {}, None)
        .unwrap()
        .unwrap()
        .get_object_id("_id")
        .unwrap();

    let update_doc = doc! {
        "$set": {
            "enqueued": null,
            "notifications": doc! {
                "__lastState": null,
                "__lastOldState": "evaluateTestType"
            }
        }
    };
    println!("{:#?}", update_doc);
    ctx.col()
        .update_one(doc! { "_id": oid }, update_doc, None)
        .unwrap();
    let res = common::get_rows(ctx.col().find(doc! { "_id": oid }, None).unwrap());
    println!("{:?}", res[0]);
    assert_eq!(res[0].get("enqueued").unwrap(), &Bson::Null);
}

#[test]
fn test_update_with_provided_oid() {
    let ctx = common::setup();

    let res = ctx
        .col()
        .insert_one(doc! { "_id": 1, "letter": "a" }, None)
        .unwrap();
    let oid = res.inserted_id;

    ctx.col()
        .update_one(
            doc! { "_id": &oid },
            doc! { "$set": { "letter": "b" } },
            None,
        )
        .unwrap();

    let res = ctx.col().find(None, None).unwrap();
    let rows = common::get_rows(res);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get_str("letter").unwrap(), "b");
}

#[test]
fn test_update_with_add_to_set() {
    let ctx = common::setup();

    let res = ctx
        .col()
        .insert_one(doc! { "letters": ["a", "b", "c"] }, None)
        .unwrap();
    let oid = res.inserted_id;

    ctx.col()
        .update_one(
            doc! { "_id": &oid },
            doc! { "$addToSet": { "letters": "d" } },
            None,
        )
        .unwrap();
    let res = ctx.col().find(doc! { "_id": &oid }, None).unwrap();
    let rows = common::get_rows(res);
    let letters = rows[0].get_array("letters").unwrap();
    assert_eq!(letters[0].as_str().unwrap(), "a");
    assert_eq!(letters[1].as_str().unwrap(), "b");
    assert_eq!(letters[2].as_str().unwrap(), "c");
    assert_eq!(letters[3].as_str().unwrap(), "d");
}

#[test]
fn test_update_with_add_to_set_object() {
    let ctx = common::setup();

    let res = ctx
        .col()
        .insert_one(doc! { "letters": [{"name": "a"}] }, None)
        .unwrap();
    let oid = res.inserted_id;

    ctx.col()
        .update_one(
            doc! { "_id": &oid },
            doc! { "$addToSet": { "letters": {"name": "a"} } },
            None,
        )
        .unwrap();
    let res = ctx.col().find(doc! { "_id": &oid }, None).unwrap();
    let rows = common::get_rows(res);
    let letters = rows[0].get_array("letters").unwrap();
    assert_eq!(letters.len(), 1);
    assert_eq!(letters[0].as_document().unwrap(), &doc! { "name": "a" });
}

#[test]
fn test_update_with_add_to_set_nested_object() {
    let ctx = common::setup();

    let res = ctx
        .col()
        .insert_one(doc! { "data": { "letters": [{"name": "a"}] } }, None)
        .unwrap();
    let oid = res.inserted_id;

    ctx.col()
        .update_one(
            doc! { "_id": &oid },
            doc! { "$addToSet": { "data.letters": {"name": "a"} } },
            None,
        )
        .unwrap();
    let res = ctx.col().find(doc! { "_id": &oid }, None).unwrap();
    let rows = common::get_rows(res);
    let letters = rows[0].get("data.letters");
    assert_eq!(letters, None);
    println!("rows[0]: {:#?}", rows[0]);
    let data = rows[0].get_document("data").unwrap();
    let letters = data.get_array("letters").unwrap();
    assert_eq!(letters.len(), 1);
    assert_eq!(letters[0].as_document().unwrap(), &doc! { "name": "a" });
}

#[test]
fn test_update_with_add_to_set_multiple_and_repeated() {
    let ctx = common::setup();

    let res = ctx
        .col()
        .insert_one(doc! { "letters": ["a", "b", "c"] }, None)
        .unwrap();
    let oid = res.inserted_id;

    ctx.col()
        .update_one(
            doc! { "_id": &oid },
            doc! { "$addToSet": { "letters": "c", "colors.selected": "red" } },
            None,
        )
        .unwrap();
    let res = ctx.col().find(doc! { "_id": &oid }, None).unwrap();
    let rows = common::get_rows(res);
    let letters = rows[0].get_array("letters").unwrap();
    assert_eq!(letters.len(), 3);
    assert_eq!(letters[0].as_str().unwrap(), "a");
    assert_eq!(letters[1].as_str().unwrap(), "b");
    assert_eq!(letters[2].as_str().unwrap(), "c");
    let colors = rows[0].get_document("colors").unwrap();
    let selected = colors.get_array("selected").unwrap();
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].as_str().unwrap(), "red");
}

#[test]
fn test_update_with_add_to_set_for_non_array() {
    let ctx = common::setup();

    let res = ctx
        .col()
        .insert_one(doc! { "_id": 1, "letters": "a" }, None)
        .unwrap();
    let oid = res.inserted_id;

    let err = ctx
        .col()
        .update_one(
            doc! { "_id": &oid },
            doc! { "$addToSet": { "letters": "c" } },
            None,
        )
        .unwrap_err();
    assert!(err.to_string().contains("Cannot apply $addToSet to a non-array field. Field named 'letters' has a non-array type int in the document _id: 1"));
}

#[test]
fn test_update_with_add_to_set_nested_for_non_array() {
    let ctx = common::setup();

    let res = ctx
        .col()
        .insert_one(doc! { "_id": 1, "letters": {"a": "1"} }, None)
        .unwrap();
    let oid = res.inserted_id;

    let err = ctx
        .col()
        .update_one(
            doc! { "_id": &oid },
            doc! { "$addToSet": { "letters.a": "2" } },
            None,
        )
        .unwrap_err();
    assert!(err.to_string().contains("Cannot apply $addToSet to a non-array field. Field named 'a' has a non-array type int in the document _id: 1"));
}
