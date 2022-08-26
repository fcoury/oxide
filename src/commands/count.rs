#![allow(dead_code)]
use crate::commands::Handler;
use crate::handler::{CommandExecutionError, Request};
use crate::pg::SqlParam;
use bson::{doc, Bson, Document};

pub struct Count {}

impl Handler for Count {
    fn new() -> Self {
        Count {}
    }

    fn handle(
        &self,
        request: &Request,
        docs: &Vec<Document>,
    ) -> Result<Document, CommandExecutionError> {
        let doc = &docs[0];
        let db = doc.get_str("$db").unwrap();
        let collection = doc.get_str("count").unwrap();
        let sp = SqlParam::new(db, collection);

        let mut client = request.get_client();

        // returns empty if db or collection doesn't exist
        if !client.table_exists(db, collection).unwrap() {
            return Ok(doc! {
                "n": 0,
                "ok": Bson::Double(1.0),
            });
        }

        let filter = if doc.contains_key("filter") {
            Some(doc.get_document("filter").unwrap().clone())
        } else {
            None
        };

        let r = client.query("SELECT COUNT(*) FROM %table%", sp, filter, &[]);
        match r {
            Ok(rows) => {
                let row = rows.iter().next().unwrap();
                let n: i64 = row.get(0);
                Ok(doc! {
                    "n": Bson::Int32(n as i32),
                    "ok": Bson::Double(1.0),
                })
            }
            Err(error) => {
                log::error!("Error during count: {:?} - doc: {}", error, &doc);
                Err(CommandExecutionError::new(format!(
                    "error during count: {:?}",
                    error
                )))
            }
        }
    }
}
