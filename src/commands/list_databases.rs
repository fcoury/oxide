use crate::commands::Handler;
use crate::pg::PgDb;
use crate::wire::UnknownCommandError;
use bson::{doc, Bson, Document};

pub struct ListDatabases {}

impl Handler for ListDatabases {
    fn new() -> Self {
        ListDatabases {}
    }

    fn handle(&self, _doc: Document) -> Result<Document, UnknownCommandError> {
        let mut client = PgDb::new();

        let mut total_size: i64 = 0;
        let mut databases: Vec<bson::Bson> = vec![];
        for schema in client.get_schemas() {
            if schema.starts_with("pg_") || schema == "information_schema" {
                continue;
            }
            let mut size: i64 = 0;
            for table in client.get_tables(&schema) {
                let db_size = client.get_table_size(&schema, &table);
                size += db_size;
                total_size += db_size;
            }
            let empty = size <= 0;
            databases.push(doc!["name": schema, "sizeOnDisk": size, "empty": empty].into());
        }

        let databases_doc = Bson::Array(databases);
        Ok(doc! {
          "databases": databases_doc,
          "totalSize": Bson::Int64(total_size),
          "totalSizeMb": Bson::Int64(total_size/1024/1024),
          "ok": Bson::Double(1.0),
        })
    }
}
