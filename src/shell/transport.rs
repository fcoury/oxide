//! One async client gives synchronous JS callbacks cancellable database operations.
use bson::{doc, Document};
use eyre::{bail, eyre, Result};
use mongodb::{options::ClientOptions, Client};
use std::{cell::RefCell, future::IntoFuture, time::Duration};
use tokio::{runtime::Runtime, sync::watch};

pub(super) struct Transport {
    client: Client,
    runtime: Runtime,
    interrupted: watch::Receiver<bool>,
    // Raw commands can expose cursors too. They live for at most one evaluation.
    cursors: RefCell<Vec<(String, String, i64)>>,
}

impl Transport {
    pub fn new(options: ClientOptions, interrupted: watch::Receiver<bool>) -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let client = runtime.block_on(async { Client::with_options(options) })?;
        Ok(Self {
            client,
            runtime,
            interrupted,
            cursors: RefCell::new(Vec::new()),
        })
    }

    pub fn command(&self, database: &str, command: Document) -> Result<Document> {
        if self.cursors.borrow().len() >= 64
            && !command.contains_key("getMore")
            && !command.contains_key("killCursors")
        {
            bail!("too many shell cursors in one evaluation");
        }
        let killed = command.get_str("killCursors").ok().map(|collection| {
            (
                collection.to_owned(),
                command.get_array("cursors").cloned().unwrap_or_default(),
            )
        });
        let old_id = command.get_i64("getMore").ok();
        let mut interrupted = self.interrupted.clone();
        let result = self.runtime.block_on(async {
            if *interrupted.borrow_and_update() { bail!("interrupted; a write may already have taken effect"); }
            let db = self.client.database(database);
            tokio::select! {
                biased;
                _ = interrupted.changed() => Err(eyre!("interrupted; a write may already have taken effect")),
                result = tokio::time::timeout(Duration::from_secs(30), db.run_command(command).into_future()) => {
                    Ok(result.map_err(|_| eyre!("database operation timed out; a write may already have taken effect"))??)
                }
            }
        })?;
        let mut cursors = self.cursors.borrow_mut();
        if let Some((collection, ids)) = killed {
            cursors.retain(|(db, col, id)| {
                db != database || col != &collection || !ids.iter().any(|v| v.as_i64() == Some(*id))
            });
        }
        if let Some(old) = old_id {
            cursors.retain(|(db, _, id)| db != database || *id != old);
        }
        if let Ok(cursor) = result.get_document("cursor") {
            let id = cursor.get_i64("id")?;
            if id != 0 {
                let ns = cursor.get_str("ns")?;
                let collection = ns
                    .strip_prefix(&format!("{database}."))
                    .ok_or_else(|| eyre!("invalid cursor namespace"))?;
                cursors.push((database.into(), collection.into(), id));
            }
        }
        Ok(result)
    }

    pub fn close_cursors(&self) {
        // Cleanup runs even after an uncatchable JS interrupt. Bound the entire
        // cleanup, and never retry writes when their outcome is unknown.
        let cursors = std::mem::take(&mut *self.cursors.borrow_mut());
        self.runtime.block_on(async {
            let _ = tokio::time::timeout(Duration::from_secs(1), async {
                for (database, collection, id) in cursors {
                    let _ = self
                        .client
                        .database(&database)
                        .run_command(doc! {"killCursors": collection, "cursors": [id]})
                        .await;
                }
            })
            .await;
        });
    }
}
