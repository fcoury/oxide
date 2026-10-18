use crate::handler::Request;
use crate::{commands::Handler, handler::CommandExecutionError};
use bson::{doc, Bson, Document};

pub struct DbStats {}

impl Handler for DbStats {
    fn new() -> Self {
        DbStats {}
    }

    fn handle(
        &self,
        request: &Request,
        docs: &[Document],
    ) -> Result<Document, CommandExecutionError> {
        crate::commands::check_options(&docs[0], &["dbStats", "scale"])?;
        let doc = &docs[0];
        let db = doc.get_str("$db")?;
        let scale = match doc.get("scale") {
            None => 1.0,
            Some(Bson::Int32(i)) => *i as f64,
            Some(Bson::Int64(i)) => *i as f64,
            Some(Bson::Double(f)) => *f,
            _ => return Err(CommandExecutionError::new("scale must be numeric".into())),
        };
        if !scale.is_finite() || scale <= 0.0 {
            return Err(CommandExecutionError::new(
                "scale must be positive and finite".into(),
            ));
        }
        let mut client = request.get_client()?;
        let stats = client.schema_stats(db, None)?;

        let table_count: i32 = stats.get("TableCount");
        let row_count: i64 = stats.get("RowCount");
        let total_size: i64 = stats.get("TotalSize");
        let index_size: i64 = stats.get("IndexSize");
        let relation_size: i64 = stats.get("RelationSize");
        let index_count: i32 = stats.get("IndexCount");

        let avg_obj_size = if row_count > 0 {
            relation_size as f64 / row_count as f64
        } else {
            0.0
        };

        Ok(doc! {
            "db": db,
            "collections": Bson::Int32(table_count),
            "views": Bson::Int32(0), // TODO
            "objects": Bson::Int64(row_count),
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
