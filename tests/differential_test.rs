//! Opt-in live comparisons. CI supplies a disposable MongoDB reference server.
use bson::{doc, Bson, Document};
use mongodb::sync::{Client, Collection};
mod common;
fn all(col: &Collection<Document>, filter: Document) -> Vec<Document> {
    common::get_rows(col.find(filter).sort(doc! {"_id":1}).run().unwrap())
}
#[test]
#[ignore = "requires MONGODB_REFERENCE_URL; executed in CI"]
fn supported_contract_matches_mongodb() {
    let ctx = common::setup();
    let client = Client::with_uri_str(
        std::env::var("MONGODB_REFERENCE_URL").expect("set a disposable MongoDB reference URL"),
    )
    .unwrap();
    let reference_db = client.database(&format!("oxide_diff_{}", uuid::Uuid::new_v4().simple()));
    let reference = reference_db.collection::<Document>("items");
    let oxide = ctx.col();
    let values = vec![
        doc! {"_id":1,"name":"O'Reilly","n":1_i64,"tags":["hello","bye"]},
        doc! {"_id":2,"n":2_i64,"tags":["other"]},
        doc! {"_id":3,"n":3_i64,"missing":Bson::Null},
    ];
    for col in [&oxide, &reference] {
        col.insert_many(values.clone()).run().unwrap();
        col.create_index(mongodb::IndexModel::builder().keys(doc! {"n":1.0}).build())
            .run()
            .unwrap();
    }
    for filter in [
        doc! {},
        doc! {"name":"O'Reilly"},
        doc! {"name' OR TRUE --":"x"},
        doc! {"tags":{"$regex":"^hell"}},
        doc! {"tags":{"$ne":"hello"}},
        doc! {"missing":Bson::Null},
        doc! {"missing":{"$ne":Bson::Null}},
    ] {
        assert_eq!(
            all(&oxide, filter.clone()),
            all(&reference, filter.clone()),
            "{filter:?}"
        );
    }
    for pipeline in [
        vec![],
        vec![
            doc! {"$sort":{"_id":1}},
            doc! {"$limit":3},
            doc! {"$skip":1},
        ],
        vec![doc! {"$project":{"_id":1,"absent":1,"missing":1,"nested.missing":1}}],
        vec![doc! {"$group":{"_id":Bson::Null,"total":{"$sum":"$n"},"missing":{"$sum":"$absent"}}}],
        vec![
            doc! {"$addFields":{"double":{"$multiply":["$n",2]},"name":"$absent","copy":"$name","explicitNull":Bson::Null}},
        ],
        vec![
            doc! {"$set":{"n":{"$add":["$n",1]},"old":"$n"}},
            doc! {"$unset":["tags","missing"]},
        ],
        vec![doc! {"$unset":"tags"}],
        vec![
            doc! {"$unwind":{"path":"$tags","preserveNullAndEmptyArrays":true,"includeArrayIndex":"position"}},
        ],
        vec![doc! {"$unwind":"$tags"}],
        vec![
            doc! {"$project":{"root":{"number":"$n"}}},
            doc! {"$replaceRoot":{"newRoot":"$root"}},
        ],
        vec![doc! {"$replaceWith":{"number":"$n"}}],
        vec![
            doc! {"$set":{"fullName":{"$concat":["$name"," / ","suffix"]},"empty":{"$concat":[]},"missing":{"$concat":["$absent","x"]}}},
        ],
        vec![doc! {"$set":{"merged":{"$mergeObjects":[{"x":1},Bson::Null,{"x":2,"y":"$n"}]} } }],
        vec![doc! {"$project":{"merged":{"$mergeObjects":[Bson::Null,"$absent"]}}}],
        vec![
            doc! {"$project":{"mapped":{"$map":{"input":"$tags","as":"tag","in":{"$cond":[{"$eq":["$$tag","hello"]},"first","other"]}}}}},
        ],
        vec![
            doc! {"$project":{"filtered":{"$filter":{"input":"$tags","as":"tag","cond":{"$eq":["$$tag","hello"]}}}}},
        ],
        vec![
            doc! {"$project":{"value":{"$let":{"vars":{"index":{"$indexOfArray":["$tags","hello"]}},"in":{"$cond":[{"$gte":["$$index",0_i64]},{"$arrayElemAt":["$tags","$$index"]},"$$REMOVE"]}}}}},
        ],
    ] {
        let get = |col: &Collection<Document>| {
            common::get_rows(col.aggregate(pipeline.clone()).run().unwrap())
        };
        assert_eq!(get(&oxide), get(&reference), "{pipeline:?}");
    }
    let mut outcomes = Vec::new();
    for col in [&oxide, &reference] {
        let before = col
            .find_one_and_update(
                doc! {"_id":1},
                doc! {"$set":{"name":"updated"},"$inc":{"n":1}},
            )
            .run()
            .unwrap();
        let noop = col
            .update_one(doc! {"_id":1}, doc! {"$set":{"name":"updated"}})
            .run()
            .unwrap();
        let replacement = col
            .replace_one(doc! {"_id":2}, doc! {"replacement":true})
            .run()
            .unwrap();
        let deleted = col.delete_one(doc! {"_id":3}).run().unwrap();
        outcomes.push((
            before,
            noop.matched_count,
            noop.modified_count,
            replacement.modified_count,
            deleted.deleted_count,
            all(col, doc! {}),
        ));
    }
    reference_db.drop().run().unwrap();
    assert_eq!(outcomes[0], outcomes[1]);
}

#[test]
#[ignore = "requires MONGODB_REFERENCE_URL; executed with MongoDB differential checks"]
fn relation_lookup_matches_mongodb() {
    let ctx = common::setup();
    let reference = Client::with_uri_str(std::env::var("MONGODB_REFERENCE_URL").unwrap()).unwrap();
    let name = format!("oxide_diff_{}", uuid::Uuid::new_v4().simple());
    let databases = [ctx.mongodb().database(&name), reference.database(&name)];
    let local = vec![
        doc! {"_id":1,"foreign_id":10_i64},
        doc! {"_id":2,"foreign_id":[10,20]},
        doc! {"_id":3},
        doc! {"_id":4,"foreign_id":Bson::Null},
    ];
    for db in &databases {
        db.collection::<Document>("items")
            .insert_many(local.clone())
            .run()
            .unwrap();
        db.collection::<Document>("foreign")
            .insert_many([
                doc! {"_id":10,"name":"A"},
                doc! {"_id":20,"name":"B"},
                doc! {"_id":30,"deleted_at":Bson::Null},
                doc! {"_id":40,"foreign_id":Bson::Null},
            ])
            .run()
            .unwrap();
    }
    for pipeline in [
        vec![
            doc! {"$lookup":{"from":"foreign","localField":"foreign_id","foreignField":"_id","as":"joined","pipeline":[{"$match":{"deleted_at":{"$exists":false}}},{"$sort":{"_id":1}}]}},
            doc! {"$sort":{"_id":1}},
        ],
        vec![
            doc! {"$lookup":{"from":"absent","localField":"foreign_id","foreignField":"_id","as":"joined"}},
            doc! {"$sort":{"_id":1}},
        ],
        vec![
            doc! {"$lookup":{"from":"foreign","localField":"foreign_id","foreignField":"foreign_id","as":"joined","pipeline":[{"$sort":{"_id":1}}]}},
            doc! {"$sort":{"_id":1}},
        ],
        vec![
            doc! {"$lookup":{"from":"foreign","localField":"foreign_id","foreignField":"_id","as":"_tmp","pipeline":[{"$sort":{"_id":1}}]}},
            doc! {"$set":{"relation":{"$let":{"vars":{"index":{"$indexOfArray":["$_tmp._id","$foreign_id"]}},"in":{"$cond":[{"$gte":["$$index",0_i64]},{"$arrayElemAt":["$_tmp","$$index"]},"$$REMOVE"]}}}}},
            doc! {"$unset":"_tmp"},
            doc! {"$sort":{"_id":1}},
        ],
    ] {
        let rows = databases
            .iter()
            .map(|db| {
                common::get_rows(
                    db.collection::<Document>("items")
                        .aggregate(pipeline.clone())
                        .run()
                        .unwrap(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(rows[0], rows[1], "{pipeline:?}");
    }
    for db in databases {
        db.drop().run().unwrap();
    }
}

#[test]
#[ignore = "requires MONGODB_REFERENCE_URL; executed with MongoDB differential checks"]
fn optional_user_identity_index_matches_mongodb() {
    let ctx = common::setup();
    let reference = Client::with_uri_str(std::env::var("MONGODB_REFERENCE_URL").unwrap()).unwrap();
    let db = reference.database(&format!("oxide_diff_{}", uuid::Uuid::new_v4().simple()));
    let columns = [ctx.col(), db.collection::<Document>("users")];
    for col in &columns {
        col.insert_many([
            doc! {"_id":1},
            doc! {"_id":2},
            doc! {"_id":3,"email":Bson::Null},
            doc! {"_id":4,"email":Bson::Null},
        ])
        .run()
        .unwrap();
        col.create_index(
            mongodb::IndexModel::builder()
                .keys(doc! {"email":1})
                .options(
                    mongodb::options::IndexOptions::builder()
                        .name("users_email_unique_when_present".to_string())
                        .unique(true)
                        .partial_filter_expression(doc! {"email":{"$type":"string"}})
                        .build(),
                )
                .build(),
        )
        .run()
        .unwrap();
        for (id, email) in [(5, "synthetic@example.invalid"), (6, "nul\0email")] {
            col.insert_one(doc! {"_id":id,"email":email}).run().unwrap();
            assert!(col
                .insert_one(doc! {"_id":id+10,"email":email})
                .run()
                .unwrap_err()
                .to_string()
                .contains("11000"));
        }
        assert_eq!(col.count_documents(doc! {}).run().unwrap(), 6);
        let index = col
            .list_indexes()
            .run()
            .unwrap()
            .map(Result::unwrap)
            .find(|index| {
                index
                    .options
                    .as_ref()
                    .and_then(|options| options.name.as_deref())
                    == Some("users_email_unique_when_present")
            })
            .unwrap();
        assert_eq!(
            index.options.unwrap().partial_filter_expression,
            Some(doc! {"email":{"$type":"string"}})
        );
    }
    assert_eq!(all(&columns[0], doc! {}), all(&columns[1], doc! {}));
    db.drop().run().unwrap();
}

#[test]
#[ignore = "requires MONGODB_REFERENCE_URL; executed with MongoDB differential checks"]
fn unwind_null_scalar_and_root_validation_match_mongodb() {
    let ctx = common::setup();
    let reference = Client::with_uri_str(std::env::var("MONGODB_REFERENCE_URL").unwrap()).unwrap();
    let db = reference.database(&format!("oxide_diff_{}", uuid::Uuid::new_v4().simple()));
    let columns = [ctx.col(), db.collection::<Document>("items")];
    for col in &columns {
        col.insert_many([
            doc! {"_id":1,"v":[1,Bson::Null]},
            doc! {"_id":2,"v":[]},
            doc! {"_id":3,"v":Bson::Null},
            doc! {"_id":4},
            doc! {"_id":5,"v":"scalar"},
        ])
        .run()
        .unwrap();
    }
    for preserve in [false, true] {
        let pipeline = vec![
            doc! {"$unwind":{"path":"$v","preserveNullAndEmptyArrays":preserve,"includeArrayIndex":"ordinal"}},
            doc! {"$sort":{"_id":1,"ordinal":1}},
        ];
        let result = columns
            .iter()
            .map(|col| common::get_rows(col.aggregate(pipeline.clone()).run().unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(result[0], result[1]);
    }
    for value in [
        Bson::Null,
        Bson::Int32(1),
        Bson::DateTime(bson::DateTime::now()),
        Bson::Array(vec![]),
        Bson::String("$absent".into()),
    ] {
        for col in &columns {
            assert!(
                col.aggregate([doc! {"$replaceRoot":{"newRoot":value.clone()}}])
                    .run()
                    .is_err(),
                "{value:?}"
            );
        }
    }
    let results = columns
        .iter()
        .map(|col| {
            common::get_rows(
                col.aggregate([doc! {"$replaceRoot":{"newRoot":{"$literal":{}}}}])
                    .run()
                    .unwrap(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(results[0], results[1]);
    db.drop().run().unwrap();
}

#[test]
#[ignore = "requires MONGODB_REFERENCE_URL; executed in CI"]
fn array_reduction_and_first_accumulator_match_mongodb() {
    let ctx = common::setup();
    let client = Client::with_uri_str(std::env::var("MONGODB_REFERENCE_URL").unwrap()).unwrap();
    let reference_db = client.database(&format!("oxide_diff_{}", uuid::Uuid::new_v4().simple()));
    let reference = reference_db.collection::<Document>("items");
    let oxide = ctx.col();
    for col in [&oxide, &reference] {
        col.insert_many(vec![
            doc! {"_id":1,"group":"a","rank":2,"arrays":[[1,2],[],[3]],"numbers":[1,2,3]},
            doc! {"_id":2,"group":"a","rank":1,"arrays":[],"numbers":[]},
            doc! {"_id":3,"group":"b","rank":2,"arrays":Bson::Null,"numbers":Bson::Null},
            doc! {"_id":4,"group":"b","rank":1},
        ])
        .run()
        .unwrap();
    }
    for pipeline in [
        vec![
            doc! {"$project":{"flat":{"$reduce":{"input":"$arrays","initialValue":[],"in":{"$concatArrays":["$$value","$$this"]}}},"sum":{"$reduce":{"input":"$numbers","initialValue":10,"in":{"$add":["$$value","$$this"]}}}}},
        ],
        vec![
            doc! {"$project":{"nested":{"$reduce":{"input":[[1,2],[3]],"initialValue":0,"in":{"$add":["$$value",{"$reduce":{"input":"$$this","initialValue":0,"in":{"$add":["$$value","$$this"]}}}]}}}}},
        ],
        vec![
            doc! {"$project":{"concat":{"$concatArrays":[[1],[2,3]]},"empty":{"$concatArrays":[]},"missing":{"$concatArrays":[[],"$absent"]},"null":{"$concatArrays":[[],Bson::Null]}}},
        ],
        vec![
            doc! {"$sort":{"rank":1,"_id":1}},
            doc! {"$group":{"_id":"$group","first":{"$first":"$$ROOT"},"missing":{"$first":"$absent"},"rank":{"$first":"$rank"}}},
            doc! {"$sort":{"_id":1}},
        ],
        vec![
            doc! {"$sort":{"rank":-1,"_id":-1}},
            doc! {"$group":{"_id":"$group","first":{"$first":"$arrays"},"rank":{"$first":"$rank"}}},
            doc! {"$sort":{"_id":1}},
        ],
    ] {
        let rows = |col: &Collection<Document>| {
            common::get_rows(col.aggregate(pipeline.clone()).run().unwrap())
        };
        assert_eq!(rows(&oxide), rows(&reference), "{pipeline:?}");
    }
    for expr in [
        doc! {"$reduce":{"input":42,"initialValue":[],"in":"$$this"}},
        doc! {"$concatArrays":[[],42]},
        doc! {"$reduce":{"input":[],"initialValue":0,"in":"$$this","unexpected":true}},
    ] {
        for col in [&oxide, &reference] {
            assert!(
                col.aggregate(vec![doc! {"$project":{"result":expr.clone()}}])
                    .run()
                    .is_err(),
                "{expr:?}"
            );
        }
    }
    reference_db.drop().run().unwrap();
}

#[test]
#[ignore = "requires MONGODB_REFERENCE_URL; executed in CI"]
fn concat_arrays_operand_boundaries_match_mongodb() {
    let ctx = common::setup();
    let client = Client::with_uri_str(std::env::var("MONGODB_REFERENCE_URL").unwrap()).unwrap();
    let db = client.database(&format!("oxide_diff_{}", uuid::Uuid::new_v4().simple()));
    let reference = db.collection::<Document>("items");
    let oxide = ctx.col();
    for col in [&oxide, &reference] {
        col.insert_one(doc! {"_id":1}).run().unwrap();
    }
    for count in [0, 100, 101, 256] {
        let operands = (0..count)
            .map(|n| Bson::Array(vec![Bson::Int32(n)]))
            .collect::<Vec<_>>();
        for suffix in [None, Some(Bson::Null), Some(Bson::String("$absent".into()))] {
            let mut inputs = operands.clone();
            inputs.extend(suffix);
            let pipeline = vec![doc! {"$project":{"values":{"$concatArrays":inputs}}}];
            let rows = |col: &Collection<Document>| {
                common::get_rows(col.aggregate(pipeline.clone()).run().unwrap())
            };
            assert_eq!(
                rows(&oxide),
                rows(&reference),
                "{count} operands: {pipeline:?}"
            );
        }
    }
    db.drop().run().unwrap();
}
