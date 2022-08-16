use bson::doc;
use mongodb::options::FindOneAndUpdateOptions;

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
        .find_one_and_update(
            doc! { "name": "Mike" },
            doc! { "$set": { "age": 44 } },
            None,
        )
        .unwrap();
    let updated = res.unwrap();

    assert_eq!(updated.get_str("name").unwrap(), "Mike");
    assert_eq!(updated.get_i32("age").unwrap(), 44);

    let rows = common::get_rows(col.find(None, None).unwrap());
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
            FindOneAndUpdateOptions::builder().upsert(true).build(),
        )
        .unwrap();
    assert_eq!(res, None);

    let rows = common::get_rows(ctx.col().find(None, None).unwrap());
    let row = rows[0].clone();
    assert_eq!(row.get_i32("age").unwrap(), 44);
}

#[test]
fn test_find_and_modify_empty() {
    let ctx = common::setup();

    let res = ctx
        .col()
        .find_one_and_update(
            doc! {"name": "Mike"},
            doc! { "$set": { "name": "Joe"}},
            None,
        )
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
        .find_one_and_update(
            doc! { "name": "John" },
            doc! { "$set": { "age": 44 } },
            FindOneAndUpdateOptions::builder()
                .sort(doc! { "age": -1 })
                .build(),
        )
        .unwrap();
    let updated = res.unwrap();

    assert_eq!(updated.get_i32("age").unwrap(), 44);
    assert_eq!(updated.get_i32("ext_id").unwrap(), 3);
}

#[test]
#[ignore = "this is not yet implemented"]
fn test_find_and_modify_inexistent_table() {}
