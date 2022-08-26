use mongodb::bson::doc;

mod common;

#[test]
fn test_count() {
    let ctx = common::setup();
    ctx.col()
        .insert_many(
            vec![
                doc! {
                    "name": "John",
                    "age": 30,
                    "city": "New York",
                },
                doc! {
                    "name": "Paul",
                    "age": 29,
                    "city": "Ann Arbor",
                },
                doc! {
                    "name": "Ella",
                    "age": 33,
                    "city": "Ann Arbor",
                },
                doc! {
                    "name": "Jane",
                    "age": 31,
                    "city": "New York",
                },
            ],
            None,
        )
        .unwrap();

    let res = ctx
        .db()
        .run_command(doc! {"count": &ctx.collection}, None)
        .unwrap();

    assert_eq!(res.get_i32("n").unwrap(), 4);
}

#[test]
fn test_count_for_non_existent_collection() {
    let ctx = common::setup();
    let res = ctx
        .db()
        .run_command(doc! {"count": "i-dont-exist"}, None)
        .unwrap();

    assert_eq!(res.get_i32("n").unwrap(), 0);
}
