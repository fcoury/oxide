use mongodb::bson::doc;

mod common;

#[test]
fn test_list_database() {
    let ctx = common::setup();

    // initially only public database is listed
    let res = ctx.mongodb().list_databases(None, None).unwrap();
    assert_eq!(res.len(), 1);
    assert_eq!(res.get(0).unwrap().name, "public");

    ctx.col().insert_one(doc! { "x": 1 }, None).unwrap();

    // lists the newly created database
    let res = ctx.mongodb().list_databases(None, None).unwrap();
    assert_eq!(res.len(), 2);
    assert!(res.get(1).unwrap().name == "test_db");
}
