//! Bounded server cursors support driver connection pooling without retaining PostgreSQL connections.
use bson::{doc, Document};
use eyre::{bail, Result};
use std::{
    collections::{HashMap, VecDeque},
    sync::atomic::{AtomicI64, Ordering},
    time::{Duration, Instant},
};
pub(crate) static NEXT_ID: AtomicI64 = AtomicI64::new(1);
const MAX_CURSORS: usize = 64;
const MAX_BATCH_BYTES: usize = 16 * 1024 * 1024;
const TTL: Duration = Duration::from_secs(60);
struct Cursor {
    namespace: String,
    documents: VecDeque<Document>,
    touched: Instant,
    bytes: usize,
}
#[derive(Default)]
pub struct Cursors {
    pub sessions: crate::session::Sessions,
    cursors: HashMap<i64, Cursor>,
    streams: HashMap<i64, crate::change_stream::Stream>,
    owners: HashMap<i64, Vec<u8>>,
}
impl Cursors {
    pub fn check_owner(&self, id: i64, owner: Option<&[u8]>) -> Result<()> {
        if self.owners.get(&id).map(Vec::as_slice) != owner {
            bail!("cursor transaction mismatch");
        }
        Ok(())
    }
    pub fn own(&mut self, reply: &Document, owner: Option<&[u8]>) -> Result<()> {
        let id = reply.get_document("cursor")?.get_i64("id")?;
        if let Some(owner) = owner {
            if id != 0 {
                self.owners.insert(id, owner.to_vec());
            }
        }
        Ok(())
    }
    pub fn end_transaction(&mut self, owner: &[u8]) {
        let ids: Vec<_> = self
            .owners
            .iter()
            .filter(|(_, value)| value.as_slice() == owner)
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            self.owners.remove(&id);
            self.cursors.remove(&id);
        }
    }

    pub fn add_stream(&mut self, stream: crate::change_stream::Stream) -> Result<i64> {
        self.expire();
        if self.cursors.len() + self.streams.len() >= MAX_CURSORS {
            bail!("server cursor budget exceeded");
        }
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        self.streams.insert(id, stream);
        Ok(id)
    }
    pub fn get_stream(
        &mut self,
        id: i64,
        namespace: &str,
    ) -> Result<Option<crate::change_stream::Stream>> {
        self.expire();
        let stream = self.streams.get(&id);
        if stream.is_some_and(|s| s.namespace != namespace) {
            bail!("cursor namespace mismatch");
        }
        Ok(stream.cloned())
    }
    pub fn update_stream(&mut self, id: i64, stream: crate::change_stream::Stream) -> Result<()> {
        let current = self
            .streams
            .get_mut(&id)
            .ok_or_else(|| eyre::eyre!("cursor was killed"))?;
        *current = stream;
        Ok(())
    }

    fn expire(&mut self) {
        self.cursors.retain(|_, c| c.touched.elapsed() < TTL);
        self.streams.retain(|_, c| c.touched.elapsed() < TTL);
        self.owners.retain(|id, _| self.cursors.contains_key(id));
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
        if total_bytes > crate::pg::MAX_SNAPSHOT_BYTES {
            bail!("cursor exceeds byte limit");
        }
        let mut documents: VecDeque<_> = documents.into();
        let first = take_batch(&mut documents, batch_size)?;
        let id = if single || documents.is_empty() {
            0
        } else {
            let bytes = documents
                .iter()
                .map(|d| bson::to_vec(d).map(|b| b.len()))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .sum::<usize>();
            if self.cursors.len() + self.streams.len() >= MAX_CURSORS
                || self.cursors.values().map(|c| c.bytes).sum::<usize>() + bytes
                    > 4 * crate::pg::MAX_SNAPSHOT_BYTES
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
        let batch = take_batch(&mut cursor.documents, batch_size)?;
        cursor.bytes -= batch
            .iter()
            .map(|d| bson::to_vec(d).map(|b| b.len()))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .sum::<usize>();
        cursor.touched = Instant::now();
        let id = if cursor.documents.is_empty() {
            self.cursors.remove(&id);
            self.owners.remove(&id);
            0
        } else {
            id
        };
        Ok(doc! {"ok":1.0,"cursor":{"id":id,"ns":namespace,"nextBatch":batch}})
    }
    pub fn kill(&mut self, id: i64, namespace: &str) -> bool {
        if self
            .streams
            .get(&id)
            .is_some_and(|s| s.namespace == namespace)
        {
            self.streams.remove(&id);
            return true;
        }
        if self
            .cursors
            .get(&id)
            .is_some_and(|c| c.namespace == namespace)
        {
            self.cursors.remove(&id);
            self.owners.remove(&id);
            true
        } else {
            false
        }
    }
}

/// Enforce the wire batch byte ceiling as well as the requested document count.
/// A single maximum-sized BSON document may fill a batch by itself.
fn take_batch(documents: &mut VecDeque<Document>, count: usize) -> Result<Vec<Document>> {
    let mut bytes = 0;
    let mut batch = Vec::new();
    while batch.len() < count {
        let Some(front) = documents.front() else {
            break;
        };
        let size = bson::to_vec(front)?.len();
        if !batch.is_empty() && bytes + size > MAX_BATCH_BYTES {
            break;
        }
        bytes += size;
        batch.push(documents.pop_front().expect("front exists"));
    }
    Ok(batch)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn byte_limited_batches_preserve_remaining_results_and_single_batch_closes() {
        let documents = (0..3)
            .map(|id| doc! {"_id":id,"payload":"x".repeat(6*1024*1024)})
            .collect::<Vec<_>>();
        let mut cursors = Cursors::default();
        let first = cursors
            .first("db.items".into(), documents.clone(), 101, false)
            .unwrap();
        let cursor = first.get_document("cursor").unwrap();
        assert_eq!(cursor.get_array("firstBatch").unwrap().len(), 2);
        let id = cursor.get_i64("id").unwrap();
        assert_ne!(id, 0);
        let next = cursors.more(id, "db.items", 101).unwrap();
        let cursor = next.get_document("cursor").unwrap();
        assert_eq!(cursor.get_i64("id").unwrap(), 0);
        assert_eq!(
            cursor.get_array("nextBatch").unwrap()[0]
                .as_document()
                .unwrap()
                .get_i32("_id")
                .unwrap(),
            2
        );
        assert!(cursors.more(id, "db.items", 101).is_err());
        let single = cursors
            .first("db.items".into(), documents, 101, true)
            .unwrap();
        let cursor = single.get_document("cursor").unwrap();
        assert_eq!(cursor.get_i64("id").unwrap(), 0);
        assert_eq!(cursor.get_array("firstBatch").unwrap().len(), 2);
    }
}
