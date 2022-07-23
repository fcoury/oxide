use bson::doc;

mod common;

#[test]
fn test_match() {
    let col = insert!(
        doc! {
            "name": "John",
            "age": 30,
            "city": "New York",
        },
        doc! {
            "name": "Paul",
            "age": 29,
            "city": "Ann Arbor",
        }
    );

    let pipeline = doc! {
        "$match": doc! {
            "age": doc! {
                "$gt": 29
            }
        }
    };

    let res = col.aggregate(vec![pipeline], None).unwrap();
    assert_unique_row_value!(res, "age", 30);
}

#[test]
fn test_group() {
    let col = insert!(
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
    );

    let pipeline = doc! {
        "$group": doc! {
            "_id": "$city",
            "age_sum": {
                "$sum": "$age"
            }
        }
    };

    let rows = common::get_rows(col.aggregate(vec![pipeline], None).unwrap());
    let ny_row = rows
        .iter()
        .find(|r| r.get_str("_id").unwrap() == "New York")
        .unwrap();
    let ann_arbor_row = rows
        .iter()
        .find(|r| r.get_str("_id").unwrap() == "Ann Arbor")
        .unwrap();
    assert_eq!(30 + 31, ny_row.get_i32("age_sum").unwrap());
    assert_eq!(29 + 33, ann_arbor_row.get_i32("age_sum").unwrap());
}

#[test]
fn test_match_group() {
    let col = insert!(
        doc! {
            "name": "John",
            "age": 30,
            "city": "New York",
            "pick": true,
        },
        doc! {
            "name": "Paul",
            "age": 29,
            "city": "Ann Arbor",
            "pick": true,
        },
        doc! {
            "name": "Ella",
            "age": 33,
            "city": "Ann Arbor",
            "pick": true,
        },
        doc! {
            "name": "Jane",
            "age": 31,
            "city": "New York",
        },
    );

    let pipelines = vec![
        doc! {
            "$match": {
                "pick": true
            },
        },
        doc! {
            "$group": {
                "_id": "$city",
                "age_sum": {
                    "$sum": "$age"
                }
            }
        },
    ];

    let rows = common::get_rows(col.aggregate(pipelines, None).unwrap());
    let ny_row = rows
        .iter()
        .find(|r| r.get_str("_id").unwrap() == "New York")
        .unwrap();
    let ann_arbor_row = rows
        .iter()
        .find(|r| r.get_str("_id").unwrap() == "Ann Arbor")
        .unwrap();
    assert_eq!(30, ny_row.get_i32("age_sum").unwrap());
    assert_eq!(29 + 33, ann_arbor_row.get_i32("age_sum").unwrap());
}
