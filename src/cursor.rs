//! Bounded server cursors support driver connection pooling without retaining PostgreSQL connections.
use bson::{doc, Document};
use eyre::{bail, Result};
use std::{
    collections::{HashMap, VecDeque},
    sync::atomic::{AtomicI64, Ordering},
    time::{Duration, Instant},
};
static NEXT_ID: AtomicI64 = AtomicI64::new(1);
const MAX_CURSORS: usize = 64;
const TTL: Duration = Duration::from_secs(60);
struct Cursor {
    namespace: String,
    documents: VecDeque<Document>,
    touched: Instant,
    bytes: usize,
}
#[derive(Default)]
pub struct Cursors {
    cursors: HashMap<i64, Cursor>,
}
impl Cursors {
    fn expire(&mut self) {
        self.cursors.retain(|_, c| c.touched.elapsed() < TTL);
    }
    pub fn first(
        &mut self,
        namespace: String,
        documents: Vec<Document>,
        batch_size: usize,
        single: bool,
    ) -> Result<Document> {
        self.expire();
        if documents.len() > crate::pg::MAX_QUERY_ROWS {
            bail!("cursor exceeds document limit");
        }
        let total_bytes = documents
            .iter()
            .try_fold(0_usize, |total, d| bson::to_vec(d).map(|b| total + b.len()))?;
        if total_bytes > crate::pg::MAX_QUERY_BYTES {
            bail!("cursor exceeds byte limit");
        }
        let mut documents: VecDeque<_> = documents.into();
        let first: Vec<_> = documents.drain(..documents.len().min(batch_size)).collect();
        let id = if single || documents.is_empty() {
            0
        } else {
            let bytes = documents
                .iter()
                .map(|d| bson::to_vec(d).map(|b| b.len()))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .sum::<usize>();
            if self.cursors.len() >= MAX_CURSORS
                || self.cursors.values().map(|c| c.bytes).sum::<usize>() + bytes
                    > 4 * crate::pg::MAX_QUERY_BYTES
            {
                bail!("server cursor budget exceeded");
            }
            let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
            self.cursors.insert(
                id,
                Cursor {
                    namespace: namespace.clone(),
                    documents,
                    touched: Instant::now(),
                    bytes,
                },
            );
            id
        };
        Ok(doc! {"ok":1.0,"cursor":{"id":id,"ns":namespace,"firstBatch":first}})
    }
    pub fn more(&mut self, id: i64, namespace: &str, batch_size: usize) -> Result<Document> {
        self.expire();
        let cursor = self
            .cursors
            .get_mut(&id)
            .ok_or_else(|| eyre::eyre!("cursor not found or expired"))?;
        if cursor.namespace != namespace {
            bail!("cursor namespace mismatch");
        }
        let batch: Vec<_> = cursor
            .documents
            .drain(..cursor.documents.len().min(batch_size))
            .collect();
        cursor.bytes -= batch
            .iter()
            .map(|d| bson::to_vec(d).map(|b| b.len()))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .sum::<usize>();
        cursor.touched = Instant::now();
        let id = if cursor.documents.is_empty() {
            self.cursors.remove(&id);
            0
        } else {
            id
        };
        Ok(doc! {"ok":1.0,"cursor":{"id":id,"ns":namespace,"nextBatch":batch}})
    }
    pub fn kill(&mut self, id: i64, namespace: &str) -> bool {
        if self
            .cursors
            .get(&id)
            .is_some_and(|c| c.namespace == namespace)
        {
            self.cursors.remove(&id);
            true
        } else {
            false
        }
    }
}
