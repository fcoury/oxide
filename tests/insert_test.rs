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
fn test_insert_without_id() {
    let ctx = common::setup();
    ctx.db()
        .run_command(
            doc! {
                "insert": &ctx.collection,
                "documents": vec![doc! {
                    "name": "Felipe"
                }]
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

#[test]
fn test_raw_jetbrains_idea_insert() {
    let ctx = common::setup();
    let count = count_documents(ctx.clone());

    fn count_documents(ctx: common::TestContext) -> usize {
        let cursor = ctx
            .mongodb()
            .database("test")
            .collection::<Document>("inventory")
            .find(None, None)
            .unwrap();
        let rows: Vec<Result<Document, mongodb::error::Error>> = cursor.collect();
        rows.len()
    }

    let insert = indoc! {"
        0000   c9 00 00 00 36 00 00 00 00 00 00 00 dd 07 00 00   ....6...........
        0010   00 00 00 00 00 33 00 00 00 02 69 6e 73 65 72 74   .....3....insert
        0020   00 0a 00 00 00 69 6e 76 65 6e 74 6f 72 79 00 08   .....inventory..
        0030   6f 72 64 65 72 65 64 00 01 02 24 64 62 00 05 00   ordered...$db...
        0040   00 00 74 65 73 74 00 00 01 80 00 00 00 64 6f 63   ..test.......doc
        0050   75 6d 65 6e 74 73 00 72 00 00 00 07 5f 69 64 00   uments.r...._id.
        0060   63 0b ac 82 29 0b 4a 69 98 af 9c ac 02 69 74 65   c...).Ji.....ite
        0070   6d 00 07 00 00 00 63 61 6e 76 61 73 00 10 71 74   m.....canvas..qt
        0080   79 00 64 00 00 00 04 74 61 67 73 00 13 00 00 00   y.d....tags.....
        0090   02 30 00 07 00 00 00 63 6f 74 74 6f 6e 00 00 03   .0.....cotton...
        00a0   73 69 7a 65 00 23 00 00 00 10 68 00 1c 00 00 00   size.#....h.....
        00b0   01 77 00 00 00 00 00 00 c0 41 40 02 75 6f 6d 00   .w.......A@.uom.
        00c0   03 00 00 00 63 6d 00 00 00                        ....cm...
    "};

    let bytes = hexdump_to_bytes(insert);
    ctx.send(&bytes);

    assert_eq!(count_documents(ctx), count + 1);
}
