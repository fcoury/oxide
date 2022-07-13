use bson::Document;
use indoc::indoc;
use mongodb::bson::doc;
use oxide::utils::hexdump_to_bytes;

mod common;

#[test]
fn test_basic_insert() {
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

#[test]
fn test_raw_kind2_op_msg_insert() {
    let ctx = common::setup();

    // FIXME count() is not working because it needs the aggregation pipeline
    //       once its done replace this helper function with it

    fn count_documents(ctx: common::TestContext) -> usize {
        let cursor = ctx
            .mongodb()
            .database("test")
            .collection::<Document>("col")
            .find(None, None)
            .unwrap();
        let rows: Vec<Result<Document, mongodb::error::Error>> = cursor.collect();
        rows.len()
    }

    let count = count_documents(ctx.clone());

    let kind2insert = indoc! {"
        0000   96 00 00 00 61 00 00 00 00 00 00 00 dd 07 00 00   ....a...........
        0010   00 00 00 00 01 2f 00 00 00 64 6f 63 75 6d 65 6e   ...../...documen
        0020   74 73 00 21 00 00 00 07 5f 69 64 00 62 ce d6 9a   ts.!...._id.b...
        0030   33 78 79 a1 ac c2 9d 40 01 78 00 00 00 00 00 00   3xy....@.x......
        0040   00 f0 3f 00 00 51 00 00 00 02 69 6e 73 65 72 74   ..?..Q....insert
        0050   00 04 00 00 00 63 6f 6c 00 08 6f 72 64 65 72 65   .....col..ordere
        0060   64 00 01 03 6c 73 69 64 00 1e 00 00 00 05 69 64   d...lsid......id
        0070   00 10 00 00 00 04 e1 54 58 c6 4e 89 4c a3 81 0f   .......TX.N.L...
        0080   19 59 d3 a3 2c cf 00 02 24 64 62 00 05 00 00 00   .Y..,...$db.....
        0090   74 65 73 74 00 00                                 test..
    "};

    ctx.send(&hexdump_to_bytes(kind2insert));
    assert_eq!(count_documents(ctx), count + 1);
}
