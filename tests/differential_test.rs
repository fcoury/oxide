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
