use bson::doc;

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
#[ignore = "this is not yet implemented"]
fn test_find_and_modify_inexistent_table() {}
