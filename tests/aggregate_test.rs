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

#[test]
fn test_group_with_sort() {
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

    let pipelines = vec![
        doc! {
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
        },
        doc! {
            "$sort": {
                "price_avg": 1
            }
        },
    ];

    let rows = common::get_rows(col.aggregate(pipelines, None).unwrap());
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
fn test_group_with_multiply() {
    let col = insert!(
        doc! {
            "store": "main",
            "qtd": 3,
            "price": 20.99,
        },
        doc! {
            "store": "main",
            "qtd": 1,
            "price": 29.99,
        },
        doc! {
            "store": "branch",
            "qtd": 2,
            "price": 14.49,
        },
    );

    let pipeline = doc! {
        "$group": doc! {
            "_id": "$store",
            "total": {
                "$sum": {
                    "$multiply": ["$qtd", "$price"]
                },
            },
        }
    };

    let rows = common::get_rows(col.aggregate([pipeline], None).unwrap());
    let main_row = rows
        .iter()
        .find(|r| r.get_str("_id").unwrap() == "main")
        .unwrap();
    assert_eq!(main_row.get_f64("total").unwrap(), 92.96);
    let branch_row = rows
        .iter()
        .find(|r| r.get_str("_id").unwrap() == "branch")
        .unwrap();
    assert_eq!(branch_row.get_f64("total").unwrap(), 28.98);
}

#[test]
fn test_group_with_subtract() {
    let col = insert!(
        doc! {
            "store": "main",
            "price": 20.99,
            "discount": 1.99,
        },
        doc! {
            "store": "main",
            "price": 29.99,
            "discount": 0.99,
        },
        doc! {
            "store": "branch",
            "price": 14.49,
            "discount": 1.49,
        },
    );

    let pipeline = doc! {
        "$group": doc! {
            "_id": "$store",
            "total": {
                "$sum": {
                    "$subtract": ["$price", "$discount"]
                },
            },
        }
    };

    let rows = common::get_rows(col.aggregate([pipeline], None).unwrap());
    let main_row = rows
        .iter()
        .find(|r| r.get_str("_id").unwrap() == "main")
        .unwrap();
    assert_eq!(main_row.get_f64("total").unwrap(), 48.00);
    let branch_row = rows
        .iter()
        .find(|r| r.get_str("_id").unwrap() == "branch")
        .unwrap();
    assert_eq!(branch_row.get_f64("total").unwrap(), 13.00);
}

#[test]
fn test_group_with_add() {
    let col = insert!(
        doc! {
            "store": "main",
            "cost": 20.99,
            "markup": 1.99,
        },
        doc! {
            "store": "main",
            "cost": 29.99,
            "markup": 0.99,
        },
        doc! {
            "store": "branch",
            "cost": 14.49,
            "markup": 1.49,
        },
    );

    let pipeline = doc! {
        "$group": doc! {
            "_id": "$store",
            "total": {
                "$sum": {
                    "$add": ["$cost", "$markup"]
                },
            },
        }
    };

    let rows = common::get_rows(col.aggregate([pipeline], None).unwrap());
    let main_row = rows
        .iter()
        .find(|r| r.get_str("_id").unwrap() == "main")
        .unwrap();
    assert_eq!(main_row.get_f64("total").unwrap(), 53.96);
    let branch_row = rows
        .iter()
        .find(|r| r.get_str("_id").unwrap() == "branch")
        .unwrap();
    assert_eq!(branch_row.get_f64("total").unwrap(), 15.98);
}

#[test]
fn test_group_with_divide() {
    let col = insert!(
        doc! {
            "store": "main",
            "total": 8.97,
            "qtd": 3,
        },
        doc! {
            "store": "main",
            "total": 37.58,
            "qtd": 2,
        },
        doc! {
            "store": "branch",
            "total": 14.49,
            "qtd": 1,
        },
    );

    let pipeline = doc! {
        "$group": doc! {
            "_id": "$store",
            "total_unit_price": {
                "$sum": {
                    "$divide": ["$total", "$qtd"]
                },
            },
        }
    };

    let rows = common::get_rows(col.aggregate([pipeline], None).unwrap());
    let main_row = rows
        .iter()
        .find(|r| r.get_str("_id").unwrap() == "main")
        .unwrap();
    assert_eq!(main_row.get_f64("total_unit_price").unwrap(), 21.78);
    let branch_row = rows
        .iter()
        .find(|r| r.get_str("_id").unwrap() == "branch")
        .unwrap();
    assert_eq!(branch_row.get_f64("total_unit_price").unwrap(), 14.49);
}

#[test]
fn test_simple_additive_projection() {
    let col = insert!(
        doc! {
            "store": "main",
            "total": 8.97,
            "qtd": 3,
        },
        doc! {
            "store": "branch 1",
            "total": 37.58,
            "qtd": 2,
        },
        doc! {
            "store": "branch 2",
            "total": 14.49,
            "qtd": 1,
        },
    );

    let pipeline = doc! {
        "$project": doc! {
            "store": 1,
            "total": 1,
        }
    };

    let rows = common::get_rows(col.aggregate([pipeline], None).unwrap());
    let main_row = rows
        .iter()
        .find(|r| r.get_str("store").unwrap() == "main")
        .unwrap();
    assert!(main_row.get("_id").is_some());
    assert_eq!("main", main_row.get_str("store").unwrap());
    assert_eq!(8.97, main_row.get_f64("total").unwrap());
    assert_eq!(None, main_row.get("qtd"));
}

#[test]
fn test_simple_exclusive_projection() {
    let col = insert!(
        doc! {
            "store": "main",
            "total": 8.97,
            "qtd": 3,
        },
        doc! {
            "store": "branch 1",
            "total": 37.58,
            "qtd": 2,
        },
        doc! {
            "store": "branch 2",
            "total": 14.49,
            "qtd": 1,
        },
    );

    let pipeline = doc! {
        "$project": doc! {
            "total": 0,
        }
    };

    let rows = common::get_rows(col.aggregate([pipeline], None).unwrap());
    let main_row = rows
        .iter()
        .find(|r| r.get_str("store").unwrap() == "main")
        .unwrap();
    assert!(main_row.get("_id").is_some());
    assert_eq!("main", main_row.get_str("store").unwrap());
    assert_eq!(3, main_row.get_i32("qtd").unwrap());
    assert_eq!(None, main_row.get("total"));
}

#[test]
fn test_match_date() {
    let col = insert!(
        doc! {
            "date": bson::DateTime::builder().year(1998).month(2).day(12).build().unwrap(),
            "value": 1,
        },
        doc! {
            "date": bson::DateTime::builder().year(1999).month(2).day(13).build().unwrap(),
            "value": 2,
        },
    );

    let pipeline = doc! {
        "$match": doc! {
            "date": {
                "$gte": bson::DateTime::builder().year(1999).month(2).day(13).build().unwrap(),
            },
        }
    };

    let rows = common::get_rows(col.aggregate([pipeline], None).unwrap());
    assert_eq!(1, rows.len());
    assert_eq!(2, rows[0].get_i32("value").unwrap());
}

#[test]
fn test_match_group_project() {
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
                },
                "age_avg": {
                    "$avg": "$age"
                }
            }
        },
        doc! {
            "$project": {
                "_id": 1,
                "age_avg": 1
            }
        },
    ];

    let rows = common::get_rows(col.aggregate(pipelines, None).unwrap());
    assert_eq!(rows.len(), 2);
    let new_york_row = rows
        .iter()
        .find(|r| r.get_str("_id").unwrap() == "New York")
        .unwrap();
    assert_eq!(
        new_york_row.keys().into_iter().collect::<Vec<&String>>(),
        vec!["_id", "age_avg"]
    );
}
