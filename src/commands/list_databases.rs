use crate::handler::Request;
use crate::pg::PgDb;
use crate::{commands::Handler, handler::CommandExecutionError};
use bson::{doc, Bson, Document};

pub struct ListDatabases {}

impl Handler for ListDatabases {
    fn new() -> Self {
        ListDatabases {}
    }

    fn handle(
        &self,
        _request: &Request,
        docs: &Vec<Document>,
    ) -> Result<Document, CommandExecutionError> {
        let doc = &docs[0];
        let name_only = doc.get_bool("nameOnly").unwrap_or(false);
        let mut client = PgDb::new();

        let mut total_size: i64 = 0;
        let mut databases: Vec<bson::Bson> = vec![];
        for schema in client.get_schemas() {
            if schema.starts_with("pg_") || schema == "information_schema" {
                continue;
            }
            if name_only {
                databases.push(doc!["name": schema].into());
            } else {
                let mut size: i64 = 0;
                for table in client.get_tables(&schema) {
                    let db_size = client.get_table_size(&schema, &table);
                    size += db_size;
                    total_size += db_size;
                }
                let empty = size <= 0;
                databases.push(doc!["name": schema, "sizeOnDisk": size, "empty": empty].into());
            }
        }

        let databases_doc = Bson::Array(databases);
        if name_only {
            Ok(doc! {
                "databases": databases_doc,
                "ok": Bson::Double(1.0),
            })
        } else {
            Ok(doc! {
                "databases": databases_doc,
                "totalSize": Bson::Int64(total_size),
                "totalSizeMb": Bson::Int64(total_size/1024/1024),
                "ok": Bson::Double(1.0),
            })
        }
    }
}
