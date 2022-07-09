use crate::handler::Request;
use crate::pg::PgDb;
use crate::{commands::Handler, handler::CommandExecutionError};
use bson::{doc, Bson, Document};

pub struct DbStats {}

impl Handler for DbStats {
    fn new() -> Self {
        DbStats {}
    }

    fn handle(
        &self,
        _request: &Request,
        docs: &Vec<Document>,
    ) -> Result<Document, CommandExecutionError> {
        let doc = &docs[0];
        let db = doc.get_str("$db").unwrap();
        let scale = doc.get_f64("scale").unwrap_or(1.0);
        let mut client = PgDb::new();
        let stats = client.schema_stats(db, None).unwrap();

        let table_count: i32 = stats.get("TableCount");
        let row_count: i32 = stats.get("RowCount");
        let total_size: i32 = stats.get("TotalSize");
        let index_size: i32 = stats.get("IndexSize");
        let relation_size: i32 = stats.get("RelationSize");
        let index_count: i32 = stats.get("IndexCount");

        let avg_obj_size = if row_count > 0 {
            relation_size as f64 / row_count as f64
        } else {
            0.0
        };

        Ok(doc! {
            "db": db,
            "collections": Bson::Int32(table_count.try_into().unwrap()),
            "views": Bson::Int32(0), // TODO
            "objects": Bson::Int32(row_count),
            "avgObjSize": Bson::Double(avg_obj_size),
            "dataSize": Bson::Double(relation_size as f64/scale),
            "indexes": Bson::Int32(index_count),
            "indexSize": Bson::Double(index_size as f64/scale),
            "totalSize": Bson::Double(total_size as f64/scale),
            "scaleFactor": scale,
            "ok": Bson::Double(1.0),
        })
    }
}
