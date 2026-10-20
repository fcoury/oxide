use bson::{doc, Bson, Document};
use mongodb::{options::IndexOptions, IndexModel};
use oxide::wire::{self, OpCode};
mod common;

fn rows(ctx: &common::TestContext, filter: Document) -> Vec<Document> {
    common::get_rows(ctx.col().find(filter).sort(doc! {"_id":1}).run().unwrap())
}
fn command(ctx: &common::TestContext, doc: Document) -> Document {
    ctx.db().run_command(doc).run().unwrap()
}
fn frame(body: Document, sequence: Option<Vec<Document>>, sequence_first: bool) -> Vec<u8> {
    let mut sections = vec![vec![0]];
    sections[0].extend(bson::to_vec(&body).unwrap());
    if let Some(documents) = sequence {
        let mut section = vec![1, 0, 0, 0, 0];
        section.extend(b"documents\0");
        for doc in documents {
            section.extend(bson::to_vec(&doc).unwrap());
        }
        let size = (section.len() - 1) as i32;
        section[1..5].copy_from_slice(&size.to_le_bytes());
        if sequence_first {
            sections.insert(0, section);
        } else {
            sections.push(section);
        }
    }
    let mut bytes = vec![0; 20];
    bytes[4..8].copy_from_slice(&42_i32.to_le_bytes());
    bytes[12..16].copy_from_slice(&2013_i32.to_le_bytes());
    for section in sections {
        bytes.extend(section);
    }
    let len = bytes.len() as i32;
    bytes[..4].copy_from_slice(&len.to_le_bytes());
    bytes
}
fn reply(bytes: &[u8]) -> Document {
    let OpCode::OpMsg(msg) = wire::parse(bytes).unwrap() else {
        panic!("expected OP_MSG")
    };
    msg.sections[0].documents[0].clone()
}

#[test]
fn filter_inputs_are_data_and_array_negation_is_existential() {
    let ctx = common::setup();
    ctx.col()
        .insert_many([
            doc! {"_id":1,"name":"O'Reilly","tags":["hello","bye"],"a":[1,2]},
            doc! {"_id":2,"name":"other","a":[2,3]},
            doc! {"_id":3,"a":Bson::Null},
        ])
        .run()
        .unwrap();
    assert_eq!(rows(&ctx, doc! {"name":"O'Reilly"}).len(), 1);
    assert!(rows(&ctx, doc! {"name' IS NOT NULL OR TRUE --":"unused"}).is_empty());
    assert_eq!(rows(&ctx, doc! {"tags":{"$regex":"^hell"}}).len(), 1);
    assert_eq!(rows(&ctx, doc! {"a":{"$ne":1}}).len(), 2);
    assert_eq!(rows(&ctx, doc! {"a":{"$nin":[1]}}).len(), 2);
    assert_eq!(rows(&ctx, doc! {"a":{"$not":{"$eq":1}}}).len(), 2);
    assert_eq!(rows(&ctx, doc! {"missing":Bson::Null}).len(), 3);
    assert!(rows(&ctx, doc! {"missing":{"$ne":Bson::Null}}).is_empty());
}

#[test]
fn updates_use_one_preimage_and_rollback_on_late_failure() {
    let ctx = common::setup();
    ctx.col()
        .insert_one(doc! {"_id":1,"state":"old","n":"invalid"})
        .run()
        .unwrap();
    assert!(ctx
        .col()
        .update_one(
            doc! {"state":"old"},
            doc! {"$set":{"state":"new"},"$inc":{"n":1}}
        )
        .run()
        .is_err());
    assert_eq!(rows(&ctx, doc! {})[0].get_str("state").unwrap(), "old");
    let result = ctx
        .col()
        .update_one(
            doc! {"state":"old"},
            doc! {"$set":{"state":"new"},"$unset":{"n":1}},
        )
        .run()
        .unwrap();
    assert_eq!((result.matched_count, result.modified_count), (1, 1));
    assert!(!rows(&ctx, doc! {})[0].contains_key("n"));
    let noop = ctx
        .col()
        .update_one(doc! {"_id":1}, doc! {"$set":{"state":"new"}})
        .run()
        .unwrap();
    assert_eq!((noop.matched_count, noop.modified_count), (1, 0));
}

#[test]
fn identity_is_unique_across_numeric_widths_and_replacement_preserves_it() {
    let ctx = common::setup();
    ctx.col()
        .insert_one(doc! {"_id":1,"old":true})
        .run()
        .unwrap();
    for id in [Bson::Int64(1), Bson::Double(1.0)] {
        let error = ctx.col().insert_one(doc! {"_id":id}).run().unwrap_err();
        assert!(error.to_string().contains("11000"));
    }
    ctx.col()
        .replace_one(doc! {"_id":1}, doc! {"new":true})
        .run()
        .unwrap();
    assert_eq!(rows(&ctx, doc! {})[0], doc! {"_id":1,"new":true});
    assert!(ctx
        .col()
        .replace_one(doc! {"_id":1}, doc! {"_id":2})
        .run()
        .is_err());
    assert_eq!(
        ctx.col()
            .delete_one(doc! {"_id":1})
            .run()
            .unwrap()
            .deleted_count,
        1
    );
}

#[test]
fn storage_round_trips_bson_tags_widths_and_special_strings() {
    let ctx = common::setup();
    let value = doc! {"_id":1,"long":1_i64,"large":i64::MAX,"double":1.0,"negative_zero":-0.0,"nul":"a\0b","date":bson::DateTime::from_millis(-1),"regex":bson::Regex{pattern:"^x'".into(),options:"im".into()},"literal":{"$o":"not-an-id","$$i":1},"decimal":bson::Decimal128::from_bytes([7;16]),"binary":bson::Binary{subtype:bson::spec::BinarySubtype::Generic,bytes:vec![0,1,255]}};
    ctx.col().insert_one(value.clone()).run().unwrap();
    let actual = rows(&ctx, doc! {})[0].clone();
    assert_eq!(actual, value);
    assert_eq!(
        actual.get_f64("negative_zero").unwrap().to_bits(),
        (-0.0_f64).to_bits()
    );
    assert_eq!(rows(&ctx, doc! {"negative_zero":0}).len(), 1);
}

#[test]
fn all_sequence_documents_are_processed_in_either_section_order() {
    for sequence_first in [false, true] {
        let ctx = common::setup();
        let bytes = frame(
            doc! {"insert":&ctx.collection,"$db":&ctx.db},
            Some(vec![doc! {"_id":1}, doc! {"_id":2}, doc! {"_id":3}]),
            sequence_first,
        );
        assert_eq!(reply(&ctx.send(&bytes)).get_i64("n").unwrap(), 3);
        assert_eq!(rows(&ctx, doc! {}).len(), 3);
    }
}

#[test]
fn ordered_and_unordered_writes_report_the_correct_indices() {
    for ordered in [false, true] {
        let ctx = common::setup();
        let result = command(
            &ctx,
            doc! {"insert":&ctx.collection,"ordered":ordered,"documents":[{"_id":1},{"_id":1},{"_id":2}]},
        );
        assert_eq!(result.get_i64("n").unwrap(), if ordered { 1 } else { 2 });
        let error = result.get_array("writeErrors").unwrap()[0]
            .as_document()
            .unwrap();
        assert_eq!(error.get_i32("index").unwrap(), 1);
        assert_eq!(error.get_i32("code").unwrap(), 11000);
    }
}

#[test]
fn find_options_and_cursors_work_across_connections() {
    let ctx = common::setup();
    ctx.col()
        .insert_many((0..8).map(|i| doc! {"_id":i,"value":i}))
        .run()
        .unwrap();
    let first = command(
        &ctx,
        doc! {"find":&ctx.collection,"sort":{"value":-1},"skip":1,"limit":3,"projection":{"value":1,"_id":0},"batchSize":1},
    );
    let cursor = first.get_document("cursor").unwrap();
    assert_eq!(
        cursor.get_array("firstBatch").unwrap(),
        &vec![Bson::Document(doc! {"value":6})]
    );
    let id = cursor.get_i64("id").unwrap();
    assert_ne!(id, 0);
    // Each raw send opens a fresh connection; server cursors are not socket-bound.
    let next = reply(&ctx.send(&frame(
        doc! {"getMore":id,"collection":&ctx.collection,"$db":&ctx.db,"batchSize":2},
        None,
        false,
    )));
    let cursor = next.get_document("cursor").unwrap();
    assert_eq!(cursor.get_i64("id").unwrap(), 0);
    assert_eq!(
        cursor.get_array("nextBatch").unwrap(),
        &vec![
            Bson::Document(doc! {"value":5}),
            Bson::Document(doc! {"value":4})
        ]
    );
    assert!(ctx
        .db()
        .run_command(doc! {"getMore":id,"collection":&ctx.collection})
        .run()
        .is_err());
}

#[test]
fn cursor_kill_and_invalid_options_are_explicit() {
    let ctx = common::setup();
    ctx.col().insert_one(doc! {"x":1}).run().unwrap();
    let first = command(&ctx, doc! {"find":&ctx.collection,"batchSize":0});
    let id = first.get_document("cursor").unwrap().get_i64("id").unwrap();
    let killed = command(&ctx, doc! {"killCursors":&ctx.collection,"cursors":[id]});
    assert_eq!(
        killed.get_array("cursorsKilled").unwrap(),
        &vec![Bson::Int64(id)]
    );
    assert!(ctx
        .db()
        .run_command(doc! {"find":&ctx.collection,"limit":"ten"})
        .run()
        .is_err());
    assert!(ctx
        .db()
        .run_command(doc! {"find":&ctx.collection,"collation":{"locale":"en"}})
        .run()
        .is_err());
}

#[test]
fn aggregate_preserves_stage_order_missing_fields_and_numeric_results() {
    let ctx = common::setup();
    ctx.col()
        .insert_many((0..8).map(|i| doc! {"_id":i,"n":1_i64}))
        .run()
        .unwrap();
    let docs = common::get_rows(
        ctx.col()
            .aggregate([
                doc! {"$sort":{"_id":1}},
                doc! {"$limit":5},
                doc! {"$skip":2},
            ])
            .run()
            .unwrap(),
    );
    assert_eq!(
        docs.iter()
            .map(|d| d.get_i32("_id").unwrap())
            .collect::<Vec<_>>(),
        vec![2, 3, 4]
    );
    assert_eq!(
        common::get_rows(ctx.col().aggregate(Vec::<Document>::new()).run().unwrap()).len(),
        8
    );
    let projected=common::get_rows(ctx.col().aggregate([doc!{"$project":{"_id":0,"missing":1,"literal":{"$literal":Bson::Null},"n":{"$add":["$n",1]}}}]).run().unwrap());
    assert_eq!(projected[0], doc! {"literal":Bson::Null,"n":2_i64});
    let grouped = common::get_rows(
        ctx.col()
            .aggregate([
                doc! {"$group":{"_id":Bson::Null,"n":{"$sum":"$n"},"missing":{"$sum":"$missing"}}},
            ])
            .run()
            .unwrap(),
    );
    assert_eq!(grouped[0].get_i64("n").unwrap(), 8);
    assert_eq!(grouped[0].get_i32("missing").unwrap(), 0);
}

#[test]
fn numeric_group_keys_compare_across_bson_types() {
    let ctx = common::setup();
    ctx.col()
        .insert_many([doc! {"n":1}, doc! {"n":1_i64}, doc! {"n":1.0}])
        .run()
        .unwrap();
    let grouped = common::get_rows(
        ctx.col()
            .aggregate([doc! {"$group":{"_id":"$n","count":{"$sum":1}}}])
            .run()
            .unwrap(),
    );
    assert_eq!(grouped.len(), 1);
    assert_eq!(grouped[0].get_i32("count").unwrap(), 3);
}

#[test]
fn unique_indexes_are_scoped_normalized_and_expand_multikey_values() {
    let first = common::setup();
    let second = common::setup();
    let model = IndexModel::builder()
        .keys(doc! {"n":1})
        .options(
            IndexOptions::builder()
                .name("same_name".to_string())
                .unique(true)
                .build(),
        )
        .build();
    for ctx in [&first, &second] {
        ctx.col().create_index(model.clone()).run().unwrap();
        ctx.col().insert_one(doc! {"n":1}).run().unwrap();
    }
    for value in [Bson::Int64(1), Bson::Double(1.0)] {
        assert!(first
            .col()
            .insert_one(doc! {"n":value})
            .run()
            .unwrap_err()
            .to_string()
            .contains("11000"));
    }
    first.col().insert_one(doc! {"other":true}).run().unwrap();
    assert!(first.col().insert_one(doc! {"n":Bson::Null}).run().is_err());
    first.col().insert_one(doc! {"n":[2,3]}).run().unwrap();
    assert!(first.col().insert_one(doc! {"n":[3,4]}).run().is_err());
    assert_eq!(first.col().list_indexes().run().unwrap().count(), 2);
    assert_eq!(second.col().list_indexes().run().unwrap().count(), 2);
}

#[test]
fn count_uses_query_and_find_and_remove_returns_the_preimage() {
    let ctx = common::setup();
    ctx.col()
        .insert_many((0..4).map(|i| doc! {"_id":i,"n":i}))
        .run()
        .unwrap();
    assert_eq!(
        command(
            &ctx,
            doc! {"count":&ctx.collection,"query":{"n":{"$gte":1}},"skip":1,"limit":1}
        )
        .get_i32("n")
        .unwrap(),
        1
    );
    let removed = ctx
        .col()
        .find_one_and_delete(doc! {})
        .sort(doc! {"n":-1})
        .run()
        .unwrap()
        .unwrap();
    assert_eq!(removed.get_i32("n").unwrap(), 3);
    assert_eq!(rows(&ctx, doc! {}).len(), 3);
}

#[test]
fn malformed_and_deep_frames_fail_without_panics() {
    let valid = frame(doc! {"ping":1,"$db":"test"}, None, false);
    for length in 0..valid.len() {
        assert!(wire::parse(&valid[..length]).is_err());
    }
    let mut invalid = valid.clone();
    invalid[..4].copy_from_slice(&(-1_i32).to_le_bytes());
    assert!(wire::parse(&invalid).is_err());
    let mut nested = doc! {};
    for _ in 0..80 {
        nested = doc! {"nested":nested};
    }
    assert!(wire::parse(&frame(nested, None, false)).is_err());
    let mut duplicate = valid.clone();
    duplicate.extend(&valid[20..]);
    let len = duplicate.len() as i32;
    duplicate[..4].copy_from_slice(&len.to_le_bytes());
    assert!(wire::parse(&duplicate).is_err());
    let ctx = common::setup();
    assert_eq!(command(&ctx, doc! {"ping":1}).get_f64("ok").unwrap(), 1.0);
}

#[test]
fn multi_update_failure_rolls_back_every_selected_document() {
    let ctx = common::setup();
    ctx.col()
        .insert_many([doc! {"_id":1,"n":1}, doc! {"_id":2,"n":"invalid"}])
        .run()
        .unwrap();
    assert!(ctx
        .col()
        .update_many(doc! {}, doc! {"$set":{"changed":true},"$inc":{"n":1}})
        .run()
        .is_err());
    assert_eq!(
        rows(&ctx, doc! {}),
        vec![doc! {"_id":1,"n":1}, doc! {"_id":2,"n":"invalid"}]
    );
}
#[test]
fn concurrent_increments_do_not_lose_writes() {
    let ctx = common::setup();
    ctx.col().insert_one(doc! {"_id":1,"n":0}).run().unwrap();
    std::thread::scope(|scope| {
        for _ in 0..8 {
            let col = ctx.col();
            scope.spawn(move || {
                for _ in 0..10 {
                    col.update_one(doc! {"_id":1}, doc! {"$inc":{"n":1}})
                        .run()
                        .unwrap();
                }
            });
        }
    });
    assert_eq!(rows(&ctx, doc! {})[0].get_i32("n").unwrap(), 80);
}
#[test]
fn large_results_return_a_bounded_error_and_limit_can_recover() {
    let ctx = common::setup();
    ctx.col()
        .insert_many((0..10).map(|i| doc! {"_id":i,"payload":"x".repeat(7_000_000)}))
        .run()
        .unwrap();
    assert!(ctx
        .col()
        .find(doc! {})
        .run()
        .unwrap_err()
        .to_string()
        .contains("byte limit"));
    assert_eq!(
        common::get_rows(ctx.col().find(doc! {}).limit(1).run().unwrap()).len(),
        1
    );
}
#[test]
fn legacy_storage_is_refused_without_modification() {
    let mut pg = oxide::pg::PgDb::try_with_uri(&std::env::var("DATABASE_URL").unwrap()).unwrap();
    let name = format!("legacy_{}", uuid::Uuid::new_v4().simple());
    pg.exec(&format!("CREATE SCHEMA IF NOT EXISTS {name}"), &[])
        .unwrap();
    pg.exec(
        &format!("CREATE TABLE IF NOT EXISTS {name}.items (_jsonb jsonb)"),
        &[],
    )
    .unwrap();
    pg.exec(
        &format!("INSERT INTO {name}.items VALUES ('{{\"old\":true}}')"),
        &[],
    )
    .unwrap();
    assert!(pg
        .create_table_if_not_exists(&name, "items")
        .unwrap_err()
        .to_string()
        .contains("not owned"));
    assert!(pg
        .find(
            &oxide::pg::SqlParam::new(&name, "items"),
            &doc! {},
            None,
            0,
            0
        )
        .unwrap_err()
        .to_string()
        .contains("unversioned"));
    assert_eq!(
        pg.raw_query(&format!("SELECT count(*) FROM {name}.items"), &[])
            .unwrap()[0]
            .get::<_, i64>(0),
        1
    );
    pg.exec(&format!("DROP SCHEMA {name} CASCADE"), &[])
        .unwrap();
}

#[test]
fn dotted_projection_omits_absent_parent_objects() {
    let ctx = common::setup();
    ctx.col()
        .insert_many([
            doc! {"_id":1},
            doc! {"_id":2,"a":{}},
            doc! {"_id":3,"a":{"b":{}}},
            doc! {"_id":4,"a":{"b":{"c":Bson::Null}}},
            doc! {"_id":5,"a":Bson::Null},
        ])
        .run()
        .unwrap();
    let result = common::get_rows(
        ctx.col()
            .aggregate([
                doc! {"$sort":{"_id":1}},
                doc! {"$project":{"_id":0,"a.b.c":1}},
            ])
            .run()
            .unwrap(),
    );
    assert_eq!(
        result,
        vec![
            doc! {},
            doc! {"a":{}},
            doc! {"a":{"b":{}}},
            doc! {"a":{"b":{"c":Bson::Null}}},
            doc! {}
        ]
    );
}
#[test]
fn oversized_document_sequences_are_rejected_during_decoding() {
    let bytes = frame(
        doc! {"insert":"items","$db":"test"},
        Some(vec![doc! {}; 10_001]),
        false,
    );
    assert!(wire::parse(&bytes)
        .unwrap_err()
        .to_string()
        .contains("10000"));
}

#[test]
fn version_one_collections_are_refused_without_rewriting_data() {
    let mut pg = oxide::pg::PgDb::try_with_uri(&std::env::var("DATABASE_URL").unwrap()).unwrap();
    let schema = format!("old_{}", uuid::Uuid::new_v4().simple());
    pg.create_schema_if_not_exists(&schema).unwrap();
    pg.batch_execute(&format!("CREATE TABLE {schema}.items (_jsonb jsonb, _id jsonb); COMMENT ON TABLE {schema}.items IS 'oxide:jsonb:v1'; INSERT INTO {schema}.items VALUES ('{{\"_id\":1,\"value\":2}}','1')")).unwrap();
    let error = pg
        .find(
            &oxide::pg::SqlParam::new(&schema, "items"),
            &doc! {},
            None,
            0,
            0,
        )
        .unwrap_err();
    assert!(error.to_string().contains("export with the old version"));
    let row=pg.raw_query(&format!("SELECT _jsonb->>'value',obj_description('{schema}.items'::regclass,'pg_class') FROM {schema}.items"),&[]).unwrap();
    assert_eq!(row[0].get::<_, String>(0), "2");
    assert_eq!(row[0].get::<_, String>(1), "oxide:jsonb:v1");
    pg.batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .unwrap();
}

#[test]
fn invalid_hints_and_general_javascript_are_rejected() {
    let ctx = common::setup();
    ctx.col().insert_one(doc! {"_id":1,"a":true}).run().unwrap();
    assert!(ctx
        .db()
        .run_command(doc! {"find":&ctx.collection,"hint":{"absent":1}})
        .run()
        .is_err());
    assert!(ctx
        .db()
        .run_command(doc! {"find":&ctx.collection,"hint":123})
        .run()
        .is_err());
    assert!(ctx
        .col()
        .find(doc! {"$where":"this.a; while(true) {}"})
        .run()
        .is_err());
}
