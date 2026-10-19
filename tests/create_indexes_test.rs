use bson::doc;
use mongodb::IndexModel;

mod common;

#[test]
fn create_indexes_test() {
    let ctx = common::setup();

    let model = IndexModel::builder().keys(doc! { "x": 1, "z": 1 }).build();
    let options = None;
    ctx.col()
        .create_index(model)
        .with_options(options)
        .run()
        .unwrap();

    let cursor = ctx.col().list_indexes().run().unwrap();
    let indexes = cursor
        .collect::<Vec<_>>()
        .iter()
        .map(|x| x.clone().unwrap())
        .collect::<Vec<_>>();
    let index = indexes
        .iter()
        .find(|x| x.keys == doc! { "x": 1, "z": 1 })
        .unwrap();
    assert_eq!(index.keys, doc! { "x": 1, "z": 1 });
}

#[test]
fn create_indexes_test_already_existing() {
    let ctx = common::setup();

    let model = IndexModel::builder().keys(doc! { "a": 1, "b": 1 }).build();
    let options = None;
    ctx.col()
        .create_index(model.clone())
        .with_options(options.clone())
        .run()
        .unwrap();
    ctx.col()
        .create_index(model)
        .with_options(options)
        .run()
        .unwrap();

    let cursor = ctx.col().list_indexes().run().unwrap();
    let indexes = cursor
        .collect::<Vec<_>>()
        .iter()
        .map(|x| x.clone().unwrap())
        .collect::<Vec<_>>();
    let index = indexes
        .iter()
        .find(|x| x.keys == doc! { "a": 1, "b": 1 })
        .unwrap();
    assert_eq!(index.keys, doc! { "a": 1, "b": 1 });
}

#[test]
fn create_nested_index() {
    let ctx = common::setup();

    let model = IndexModel::builder()
        .keys(doc! { "a.z": 1, "b.c.d": 1 })
        .build();
    let options = None;
    ctx.col()
        .create_index(model)
        .with_options(options)
        .run()
        .unwrap();

    let cursor = ctx.col().list_indexes().run().unwrap();
    let indexes = cursor
        .collect::<Vec<_>>()
        .iter()
        .map(|x| x.clone().unwrap())
        .collect::<Vec<_>>();
    let index = indexes
        .iter()
        .find(|x| x.keys == doc! { "a.z": 1, "b.c.d": 1 })
        .unwrap();
    assert_eq!(index.keys, doc! { "a.z": 1, "b.c.d": 1 });
}

#[test]
fn unique_multikey_domains_backfill_and_mutations() {
    let ctx = common::setup();
    let col = ctx.col();
    col.insert_one(doc! {"_id":1,"domains":["a.test","a.test","b.test"]})
        .run()
        .unwrap();
    let index = IndexModel::builder()
        .keys(doc! {"domains":1})
        .options(
            mongodb::options::IndexOptions::builder()
                .unique(true)
                .build(),
        )
        .build();
    col.create_index(index.clone()).run().unwrap();
    col.create_index(index).run().unwrap();
    let error = col
        .insert_one(doc! {"_id":2,"domains":["c.test","a.test"]})
        .run()
        .unwrap_err();
    assert!(error.to_string().contains("11000"), "{error}");
    assert_eq!(col.count_documents(doc! {}).run().unwrap(), 1);
    col.update_one(doc! {"_id":1}, doc! {"$set":{"domains":["b.test"]}})
        .run()
        .unwrap();
    col.insert_one(doc! {"_id":2,"domains":["a.test"]})
        .run()
        .unwrap();
    assert!(col
        .update_one(doc! {"_id":2}, doc! {"$set":{"domains":["b.test"]}})
        .run()
        .is_err());
    assert_eq!(
        col.find_one(doc! {"_id":2})
            .run()
            .unwrap()
            .unwrap()
            .get_array("domains")
            .unwrap(),
        &vec![bson::Bson::String("a.test".into())]
    );
    col.delete_one(doc! {"_id":1}).run().unwrap();
    col.insert_one(doc! {"domains":["b.test"]}).run().unwrap();
}

#[test]
fn unique_multikey_duplicate_backfill_is_atomic() {
    let ctx = common::setup();
    ctx.col()
        .insert_many([doc! {"domains":["same"]}, doc! {"domains":["same"]}])
        .run()
        .unwrap();
    let index = IndexModel::builder()
        .keys(doc! {"domains":1})
        .options(
            mongodb::options::IndexOptions::builder()
                .unique(true)
                .build(),
        )
        .build();
    assert!(ctx.col().create_index(index).run().is_err());
    assert_eq!(ctx.col().list_indexes().run().unwrap().count(), 1);
    ctx.col()
        .insert_one(doc! {"domains":["same"]})
        .run()
        .unwrap();
}

#[test]
fn unique_multikey_concurrent_insert_has_one_owner() {
    let ctx = common::setup();
    ctx.col()
        .create_index(
            IndexModel::builder()
                .keys(doc! {"domains":1})
                .options(
                    mongodb::options::IndexOptions::builder()
                        .unique(true)
                        .build(),
                )
                .build(),
        )
        .run()
        .unwrap();
    let handles: Vec<_> = (0..8)
        .map(|id| {
            let col = ctx.col();
            std::thread::spawn(move || {
                col.insert_one(doc! {"_id":id,"domains":["shared"]})
                    .run()
                    .is_ok()
            })
        })
        .collect();
    assert_eq!(
        handles
            .into_iter()
            .filter_map(|h| h.join().unwrap().then_some(()))
            .count(),
        1
    );
}

#[test]
fn dropping_a_multikey_collection_removes_its_key_storage() {
    let ctx = common::setup();
    let col = ctx.col();
    let model = IndexModel::builder()
        .keys(doc! {"domains":1})
        .options(
            mongodb::options::IndexOptions::builder()
                .unique(true)
                .build(),
        )
        .build();
    col.create_index(model.clone()).run().unwrap();
    col.insert_one(doc! {"domains":["same"]}).run().unwrap();
    col.drop().run().unwrap();
    col.create_index(model).run().unwrap();
    col.insert_one(doc! {"domains":["same"]}).run().unwrap();
    assert_eq!(
        ctx.db()
            .list_collection_names()
            .run()
            .unwrap()
            .iter()
            .filter(|name| name.starts_with("ox_"))
            .count(),
        0
    );
}
