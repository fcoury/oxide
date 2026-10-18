use bson::doc;
use mongodb::options::{FindOneAndUpdateOptions, ReturnDocument};

mod common;

#[test]
fn test_find_and_modify() {
    let col = insert! {
        doc! {
            "name": "John",
            "age": 32,
        },
        doc! {
            "name": "Sheila",
            "age": 22,
        },
        doc! {
            "name": "Mike",
            "age": 87,
        }
    };

    let res = col
        .find_one_and_update(doc! { "name": "Mike" }, doc! { "$set": { "age": 44 } })
        .run()
        .unwrap();
    let updated = res.unwrap();

    assert_eq!(updated.get_str("name").unwrap(), "Mike");
    assert_eq!(updated.get_i32("age").unwrap(), 87);

    let rows = common::get_rows(col.find(bson::doc! {}).run().unwrap());
    let mike = rows
        .iter()
        .find(|r| r.get_str("name").unwrap() == "Mike")
        .unwrap();
    assert_eq!(mike.get_i32("age").unwrap(), 44);
}

#[test]
fn test_find_and_modify_upsert() {
    let ctx = common::setup();

    let res = ctx
        .col()
        .find_one_and_update(
            doc! { "name": "Mike", "active": "true" },
            doc! { "$set": { "age": 44 } },
        )
        .with_options(FindOneAndUpdateOptions::builder().upsert(true).build())
        .run()
        .unwrap();
    assert_eq!(res, None);

    let rows = common::get_rows(ctx.col().find(bson::doc! {}).run().unwrap());
    let row = rows[0].clone();
    assert_eq!(row.get_i32("age").unwrap(), 44);
}

#[test]
fn test_find_and_modify_upsert_with_returning_new() {
    let ctx = common::setup();

    let res = ctx
        .col()
        .find_one_and_update(
            doc! { "name": "Mike", "active": true },
            doc! { "$set": { "age": 44 } },
        )
        .with_options(
            FindOneAndUpdateOptions::builder()
                .upsert(true)
                .return_document(ReturnDocument::After)
                .build(),
        )
        .run()
        .unwrap()
        .unwrap();
    assert_eq!(res.get_str("name").unwrap(), "Mike");
    assert_eq!(res.get_bool("active").unwrap(), true);
    assert_eq!(res.get_i32("age").unwrap(), 44);

    let rows = common::get_rows(ctx.col().find(bson::doc! {}).run().unwrap());
    let row = rows[0].clone();
    assert_eq!(row.get_i32("age").unwrap(), 44);
}

#[test]
fn test_find_and_modify_empty() {
    let ctx = common::setup();

    let res = ctx
        .col()
        .find_one_and_update(doc! {"name": "Mike"}, doc! { "$set": { "name": "Joe"}})
        .run()
        .unwrap();

    assert_eq!(res, None);
}

#[test]
fn test_find_and_modify_with_sort() {
    let col = insert! {
        doc! {
            "ext_id": 1,
            "name": "John",
            "age": 45,
        },
        doc! {
            "ext_id": 2,
            "name": "John",
            "age": 22,
        },
        doc! {
            "ext_id": 3,
            "name": "John",
            "age": 87,
        }
    };

    let res = col
        .find_one_and_update(doc! { "name": "John" }, doc! { "$set": { "age": 44 } })
        .with_options(
            FindOneAndUpdateOptions::builder()
                .sort(doc! { "age": -1 })
                .build(),
        )
        .run()
        .unwrap();
    let updated = res.unwrap();

    assert_eq!(updated.get_i32("age").unwrap(), 87);
    assert_eq!(updated.get_i32("ext_id").unwrap(), 3);
}

#[test]
fn test_find_and_modify_inexistent_table() {
    let ctx = common::setup();
    assert!(ctx
        .col()
        .find_one_and_update(doc! {}, doc! {"$set":{"x":1}})
        .run()
        .unwrap()
        .is_none());
}
