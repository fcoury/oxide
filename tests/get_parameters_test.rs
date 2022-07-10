use mongodb::bson::{doc, Bson};

mod common;

#[test]
fn get_parameters_selected_params_test() {
    let ctx = common::setup();

    let res = ctx
        .db()
        .run_command(
            doc! { "getParameter": 1, "acceptApiVersion2": 1, "authSchemaVersion": 1 },
            None,
        )
        .unwrap();
    assert_eq!(
        res,
        doc! {
            "acceptApiVersion2": false,
            "authSchemaVersion": 5,
            "ok": Bson::Double(1.0),
        }
    );
}

#[test]
fn get_parameters_selected_params_with_details_test() {
    let ctx = common::setup();

    let res = ctx
        .db()
        .run_command(
            doc! {
                "getParameter": doc! { "showDetails":true },
                "featureCompatibilityVersion": 1,
                "quiet": 1
            },
            None,
        )
        .unwrap();
    assert_eq!(
        res,
        doc! {
            "featureCompatibilityVersion": doc! {
                "value": Bson::Double(5.0),
                "settableAtRuntime": true,
                "settableAtStartup": true,
            },
            "quiet": doc! {
                "value": false,
                "settableAtRuntime": true,
                "settableAtStartup": true,
            },
            "ok": Bson::Double(1.0),
        }
    );
}

#[test]
fn get_parameters_all_test() {
    let ctx = common::setup();

    let res = ctx
        .db()
        .run_command(doc! { "getParameter": "*" }, None)
        .unwrap();
    assert_eq!(
        res,
        doc! {
            "acceptApiVersion2": false,
            "authSchemaVersion": 5,
            "tlsMode": "disabled",
            "sslMode": "disabled",
            "quiet": false,
            "featureCompatibilityVersion": Bson::Double(5.0),
            "ok": Bson::Double(1.0),
        }
    );
}

#[test]
fn get_parameters_all_test_with_details() {
    let ctx = common::setup();

    let res = ctx
        .db()
        .run_command(
            doc! { "getParameter": doc!{ "allParameters": true, "showDetails": true } },
            None,
        )
        .unwrap();
    assert_eq!(
        res,
        doc! {
            "acceptApiVersion2": doc! {
                "value": false,
                "settableAtRuntime": true,
                "settableAtStartup": true,
            },
            "authSchemaVersion": doc! {
                "value": Bson::Int32(5),
                "settableAtRuntime": true,
                "settableAtStartup": true,
            },
            "tlsMode": doc! {
                "value": "disabled",
                "settableAtRuntime": true,
                "settableAtStartup": false,
            },
            "sslMode": doc! {
                "value": "disabled",
                "settableAtRuntime": true,
                "settableAtStartup": false,
            },
            "quiet": doc! {
                "value": false,
                "settableAtRuntime": true,
                "settableAtStartup": true,
            },
            "featureCompatibilityVersion": doc! {
                "value": Bson::Double(5.0),
                "settableAtRuntime": true,
                "settableAtStartup": true,
            },
            "ok": Bson::Double(1.0),
        }
    );
}
