use mongodb::bson::{doc, Bson};
use oxide::wire::MAX_DOCUMENT_LEN;

mod common;

#[test]
fn basic_build_info_test() {
    let ctx = common::setup();

    let res = ctx.db().run_command(doc! { "buildInfo": 1 }, None).unwrap();
    println!("{:?}", res);
    assert_eq!(res.get_str("version").unwrap(), "5.0.42");
    assert_eq!(
        res.get_str("gitVersion").unwrap(),
        "30cf72e1380e1732c0e24016f092ed58e38eeb58"
    );
    assert_eq!(res.get_array("modules").unwrap(), &[]);
    assert_eq!(res.get_str("sysInfo").unwrap(), "deprecated");
    assert_eq!(
        res.get_array("versionArray").unwrap(),
        &[
            Bson::Int32(5),
            Bson::Int32(0),
            Bson::Int32(42),
            Bson::Int32(0)
        ]
    );
    assert_eq!(res.get_i32("bits").unwrap(), 64 as i32);
    assert_eq!(res.get_bool("debug").unwrap(), false);
    assert_eq!(
        res.get_i32("maxBsonObjectSize").unwrap(),
        MAX_DOCUMENT_LEN as i32
    );
    assert_eq!(res.get_document("buildEnvironment").unwrap(), &doc! {});
    assert_eq!(res.get_f64("ok").unwrap(), 1.0);
}
