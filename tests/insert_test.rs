use mongodb::bson::doc;

mod common;

#[test]
fn basic_insert_test() {
    let ctx = common::setup();

    ctx.col()
        .insert_many(vec![doc! { "x": 1 }, doc! { "x": 2 }], None)
        .unwrap();

    let mut cursor = ctx.col().find(None, None).unwrap();
    let row1 = cursor.next().unwrap().unwrap();
    assert_eq!(row1.get_i32("x").unwrap(), 1);
    let row2 = cursor.next().unwrap().unwrap();
    assert_eq!(row2.get_i32("x").unwrap(), 2);
    assert!(cursor.next().is_none());
}
