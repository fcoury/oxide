use crate::handler::CommandExecutionError;
use crate::pg::{PgDb, SqlParam};
use crate::{commands::Handler, handler::Request};
use bson::{doc, Bson, Document};

pub struct CollStats {}

impl Handler for CollStats {
    fn new() -> Self {
        CollStats {}
    }

    fn handle(&self, _: &Request, docs: &Vec<Document>) -> Result<Document, CommandExecutionError> {
        let doc = &docs[0];
        let sp = SqlParam::from(&doc, "collStats");

        let mut client = PgDb::new();
        let stats = client.schema_stats(&sp.db, Some(&sp.collection)).unwrap();

        Ok(doc! {
            "ns": sp.to_string(),
            "count": Bson::Int32(stats.get("RowCount")),
            "size": Bson::Int32(stats.get("TotalSize")),
            "storageSize": Bson::Int32(stats.get("RelationSize")),
            "totalIndexSize": Bson::Int32(stats.get("IndexSize")),
            "totalSize": Bson::Int32(stats.get("TotalSize")),
            "scaleFactor": Bson::Int32(1),
            "ok": Bson::Double(1.0),
        })
    }
}
