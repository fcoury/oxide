use bson::{doc, Document};
use mongodb::{
    change_stream::event::ChangeStreamEvent, options::FullDocumentType, sync::ChangeStream,
};
use std::time::{Duration, Instant};
mod common;

// A live change stream may return empty batches while the journal publisher is
// busy. Bound the wait without treating a single empty batch as lost data.
fn next_change(
    stream: &mut ChangeStream<ChangeStreamEvent<Document>>,
) -> ChangeStreamEvent<Document> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(change) = stream.next_if_any().unwrap() {
            return change;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for a change event"
        );
    }
}
#[test]
fn committed_changes_resume_without_skipping_small_batches() {
    let ctx = common::setup();
    let col = ctx.col();
    col.insert_one(doc! {"_id":1,"name":"tenant-a","enabled":false})
        .run()
        .unwrap();
    let mut stream = col
        .watch()
        .full_document(FullDocumentType::UpdateLookup)
        .batch_size(1)
        .max_await_time(Duration::from_millis(20))
        .run()
        .unwrap();
    assert!(stream.next_if_any().unwrap().is_none());
    col.update_one(doc! {"_id":1}, doc! {"$set":{"enabled":true}})
        .run()
        .unwrap();
    let change = next_change(&mut stream);
    assert_eq!(
        change.full_document.as_ref().unwrap().get_bool("enabled"),
        Ok(true)
    );
    let resume = stream.resume_token().unwrap();
    let mut session = ctx.mongodb().start_session().run().unwrap();
    session.start_transaction().run().unwrap();
    col.insert_one(doc! {"_id":2})
        .session(&mut session)
        .run()
        .unwrap();
    assert!(stream.next_if_any().unwrap().is_none());
    session.abort_transaction().run().unwrap();
    assert!(stream.next_if_any().unwrap().is_none());
    session.start_transaction().run().unwrap();
    col.insert_one(doc! {"_id":3})
        .session(&mut session)
        .run()
        .unwrap();
    assert!(stream.next_if_any().unwrap().is_none());
    session.commit_transaction().run().unwrap();
    col.insert_many((4..8).map(|id| doc! {"_id":id}))
        .run()
        .unwrap();
    for id in 3..8 {
        assert_eq!(
            next_change(&mut stream)
                .document_key
                .unwrap()
                .get_i32("_id"),
            Ok(id)
        );
    }
    drop(stream);
    let mut stream = col
        .watch()
        .resume_after(resume)
        .batch_size(1)
        .max_await_time(Duration::from_millis(20))
        .run()
        .unwrap();
    for id in 3..8 {
        assert_eq!(
            next_change(&mut stream)
                .document_key
                .unwrap()
                .get_i32("_id"),
            Ok(id)
        );
    }
}

#[test]
fn expired_journal_resume_token_fails_explicitly() {
    let ctx = common::setup_with_pg_db("stream_retention", false);
    let col = ctx.col();
    col.insert_one(doc! {"_id":1}).run().unwrap();
    let mut stream = col
        .watch()
        .max_await_time(Duration::from_millis(10))
        .run()
        .unwrap();
    assert!(stream.next_if_any().unwrap().is_none());
    col.insert_one(doc! {"_id":2}).run().unwrap();
    next_change(&mut stream);
    let token = bson::to_bson(&stream.resume_token().unwrap()).unwrap();
    col.insert_one(doc! {"_id":3}).run().unwrap();
    // Consume the newer event before aging the journal. A best-effort pump can
    // skip work while another publisher holds the lock, leaving the floor equal
    // to the saved token rather than advancing beyond it.
    assert_eq!(
        next_change(&mut stream)
            .document_key
            .unwrap()
            .get_i32("_id"),
        Ok(3)
    );
    drop(stream);
    let uri = format!(
        "{}/stream_retention_{}",
        std::env::var("TEST_DATABASE_URL").unwrap(),
        std::process::id()
    );
    let mut pg = oxide::pg::PgDb::try_with_uri(&uri).unwrap();
    pg.exec(
        "UPDATE oxide_internal.changes SET created=clock_timestamp()-interval '48 hours'",
        &[],
    )
    .unwrap();
    oxide::change_stream::prune(&mut pg).unwrap();
    let error=ctx.db().run_command(doc!{"aggregate":&ctx.collection,"pipeline":[{"$changeStream":{"resumeAfter":token}}],"cursor":{}}).run().unwrap_err();
    assert!(error.to_string().contains("286"), "{error}");
}

#[test]
fn independently_committed_hook_write_is_delivered_before_outer_transaction() {
    let ctx = common::setup_with_pg_db("stream_commit_order", false);
    let col = ctx.col();
    col.insert_one(doc! {"_id":0}).run().unwrap();
    let mut stream = col
        .watch()
        .max_await_time(Duration::from_millis(100))
        .run()
        .unwrap();
    assert!(stream.next_if_any().unwrap().is_none());
    let mut outer = ctx.mongodb().start_session().run().unwrap();
    outer.start_transaction().run().unwrap();
    col.insert_one(doc! {"_id":1})
        .session(&mut outer)
        .run()
        .unwrap();
    // Sequence hooks can write on a separate connection, outside the entity
    // transaction. They must complete without waiting for that transaction.
    col.insert_one(doc! {"_id":2}).run().unwrap();
    assert_eq!(
        next_change(&mut stream)
            .document_key
            .unwrap()
            .get_i32("_id"),
        Ok(2)
    );
    outer.commit_transaction().run().unwrap();
    assert_eq!(
        next_change(&mut stream)
            .document_key
            .unwrap()
            .get_i32("_id"),
        Ok(1)
    );
}
