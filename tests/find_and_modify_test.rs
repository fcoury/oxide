use bson::doc;

mod common;

#[test]
fn test_find_and_modify() {
    let col = insert! {
        doc! {
            "name": "Mike",
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

    col.find_one_and_update(
        doc! { "name": "Mike" },
        doc! { "$set": { "age": 44 } },
        None,
    )
    .unwrap();

    let rows = common::get_rows(col.find(None, None).unwrap());
    assert_eq!(rows[0].get_i32("age").unwrap(), 44);
    assert_eq!(rows[2].get_i32("age").unwrap(), 87);
}
