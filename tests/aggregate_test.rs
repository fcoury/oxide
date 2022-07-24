use bson::doc;
use chrono::{TimeZone, Utc};

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

#[test]
fn test_group_match() {
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

    let pipelines = vec![
        doc! {
            "$group": {
                "_id": "$city",
                "age_sum": {
                    "$sum": "$age"
                }
            },
        },
        doc! {
            "$match": {
                "age_sum": {
                    "$gt": 61
                }
            }
        },
    ];

    let rows = common::get_rows(col.aggregate(pipelines, None).unwrap());
    let ann_arbor_row = rows
        .iter()
        .find(|r| r.get_str("_id").unwrap() == "Ann Arbor")
        .unwrap();
    assert_eq!(29 + 33, ann_arbor_row.get_i32("age_sum").unwrap());
}

#[test]
fn test_group_with_date() {
    let dt1 = Utc.ymd(2020, 1, 1).and_hms_milli(10, 0, 1, 444);
    let dt2 = Utc.ymd(2021, 12, 12).and_hms_milli(10, 42, 19, 0);

    let col = insert!(
        doc! {
            "date": bson::DateTime::from_millis(dt1.timestamp_millis()),
            "qtd": 3,
            "price": 20.99,
        },
        doc! {
            "date": bson::DateTime::from_millis(dt1.timestamp_millis()),
            "qtd": 1,
            "price": 29.99,
        },
        doc! {
            "date": bson::DateTime::from_millis(dt2.timestamp_millis()),
            "qtd": 2,
            "price": 14.49,
        },
    );

    let pipeline = doc! {
        "$group": doc! {
            "_id": {
                "$dateToString": {
                    "format": "%Y",
                    "date": "$date"
                }
            },
            "qtd_sum": {
                "$sum": "$qtd"
            },
            "price_avg": {
                "$avg": "$price"
            },
        }
    };

    let rows = common::get_rows(col.aggregate([pipeline], None).unwrap());
    let first_date_row = rows
        .iter()
        .find(|r| r.get_str("_id").unwrap() == "2020-01-01")
        .unwrap();
    assert_eq!(first_date_row.get_i32("qtd_sum").unwrap(), 4);
    assert_eq!(first_date_row.get_f64("price_avg").unwrap(), 25.49);
    let second_date_row = rows
        .iter()
        .find(|r| r.get_str("_id").unwrap() == "2021-12-12")
        .unwrap();
    assert_eq!(second_date_row.get_i32("qtd_sum").unwrap(), 2);
    assert_eq!(second_date_row.get_f64("price_avg").unwrap(), 14.49);
}

#[test]
fn test_order() {
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
        "$sort": doc! {
            "age": 1
        }
    };

    let rows = common::get_rows(col.aggregate(vec![pipeline], None).unwrap());
    let first_row = rows.get(0).unwrap();
    let second_row = rows.get(1).unwrap();
    assert_eq!(29, first_row.get_i32("age").unwrap());
    assert_eq!(30, second_row.get_i32("age").unwrap());

    let pipeline = doc! {
        "$sort": doc! {
            "age": -1
        }
    };

    let rows = common::get_rows(col.aggregate(vec![pipeline], None).unwrap());
    let first_row = rows.get(0).unwrap();
    let second_row = rows.get(1).unwrap();
    assert_eq!(30, first_row.get_i32("age").unwrap());
    assert_eq!(29, second_row.get_i32("age").unwrap());
}
