use bson::doc;
mod common;

#[test]
fn transactions_commit_abort_and_isolate_across_commands() {
    let ctx = common::setup_with_pg_db("session_commit", false);
    let col = ctx.col();
    col.insert_one(doc! {"_id":1,"n":0}).run().unwrap();
    let history = ctx
        .db()
        .collection::<bson::Document>(&format!("{}_history", ctx.collection));
    let mut session = ctx.mongodb().start_session().run().unwrap();
    session.start_transaction().run().unwrap();
    col.update_one(doc! {"_id":1}, doc! {"$inc":{"n":1}})
        .session(&mut session)
        .run()
        .unwrap();
    history
        .insert_one(doc! {"_id":1})
        .session(&mut session)
        .run()
        .unwrap();
    assert_eq!(
        col.find_one(doc! {"_id":1})
            .run()
            .unwrap()
            .unwrap()
            .get_i32("n"),
        Ok(0)
    );
    assert_eq!(
        col.find_one(doc! {"_id":1})
            .session(&mut session)
            .run()
            .unwrap()
            .unwrap()
            .get_i32("n"),
        Ok(1)
    );
    session.abort_transaction().run().unwrap();
    assert_eq!(
        col.find_one(doc! {"_id":1})
            .run()
            .unwrap()
            .unwrap()
            .get_i32("n"),
        Ok(0)
    );
    assert_eq!(history.count_documents(doc! {}).run().unwrap(), 0);
    session.start_transaction().run().unwrap();
    col.update_one(doc! {"_id":1}, doc! {"$inc":{"n":2}})
        .session(&mut session)
        .run()
        .unwrap();
    history
        .insert_one(doc! {"_id":2})
        .session(&mut session)
        .run()
        .unwrap();
    session.commit_transaction().run().unwrap();
    session.commit_transaction().run().unwrap();
    assert_eq!(
        col.find_one(doc! {"_id":1})
            .run()
            .unwrap()
            .unwrap()
            .get_i32("n"),
        Ok(2)
    );
    assert_eq!(history.count_documents(doc! {}).run().unwrap(), 1);
}

#[test]
fn duplicate_key_aborts_whole_transaction() {
    let ctx = common::setup_with_pg_db("session_duplicate", false);
    let col = ctx.col();
    col.insert_one(doc! {"_id":1}).run().unwrap();
    let mut session = ctx.mongodb().start_session().run().unwrap();
    session.start_transaction().run().unwrap();
    col.insert_one(doc! {"_id":2})
        .session(&mut session)
        .run()
        .unwrap();
    assert!(col
        .insert_one(doc! {"_id":1})
        .session(&mut session)
        .run()
        .is_err());
    assert!(session.commit_transaction().run().is_err());
    assert_eq!(col.count_documents(doc! {}).run().unwrap(), 1);
}

#[test]
fn snapshot_write_conflict_has_transient_label_and_can_retry() {
    let ctx = common::setup_with_pg_db("session_conflict", false);
    let col = ctx.col();
    col.insert_one(doc! {"_id":1,"n":0}).run().unwrap();
    let mut stale = ctx.mongodb().start_session().run().unwrap();
    stale.start_transaction().run().unwrap();
    col.find_one(doc! {"_id":1})
        .session(&mut stale)
        .run()
        .unwrap();
    col.update_one(doc! {"_id":1}, doc! {"$inc":{"n":1}})
        .run()
        .unwrap();
    let error = col
        .update_one(doc! {"_id":1}, doc! {"$inc":{"n":1}})
        .session(&mut stale)
        .run()
        .unwrap_err();
    assert!(
        error.contains_label("TransientTransactionError"),
        "{error:?}"
    );
    let _ = stale.abort_transaction().run();
    stale.start_transaction().run().unwrap();
    col.update_one(doc! {"_id":1}, doc! {"$inc":{"n":1}})
        .session(&mut stale)
        .run()
        .unwrap();
    stale.commit_transaction().run().unwrap();
    assert_eq!(
        col.find_one(doc! {"_id":1})
            .run()
            .unwrap()
            .unwrap()
            .get_i32("n"),
        Ok(2)
    );
}

#[test]
fn transaction_cursor_is_private_and_closed_on_abort() {
    let ctx = common::setup_with_pg_db("session_cursor", false);
    ctx.col()
        .insert_many([doc! {"_id":1}, doc! {"_id":2}])
        .run()
        .unwrap();
    let mut session = ctx.mongodb().start_session().run().unwrap();
    session.start_transaction().run().unwrap();
    let result = ctx
        .db()
        .run_command(doc! {"find":&ctx.collection,"batchSize":0})
        .session(&mut session)
        .run()
        .unwrap();
    let id = result
        .get_document("cursor")
        .unwrap()
        .get_i64("id")
        .unwrap();
    assert_ne!(id, 0);
    let get = doc! {"getMore":id,"collection":&ctx.collection};
    assert!(ctx.db().run_command(get.clone()).run().is_err());
    session.abort_transaction().run().unwrap();
    assert!(ctx.db().run_command(get).run().is_err());
}
