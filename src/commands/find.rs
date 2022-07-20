#![allow(dead_code)]
use crate::deserializer::PostgresJsonDeserializer;
use crate::handler::{CommandExecutionError, Request};
use crate::{commands::Handler, pg::SqlParam};
use bson::{doc, Bson, Document};

pub struct Find {}

impl Handler for Find {
    fn new() -> Self {
        Find {}
    }

    fn handle(
        &self,
        request: &Request,
        docs: &Vec<Document>,
    ) -> Result<Document, CommandExecutionError> {
        let doc = &docs[0];
        let db = doc.get_str("$db").unwrap();
        let collection = doc.get_str("find").unwrap();
        let sp = SqlParam::new(db, collection);

        let mut client = request.get_client();

        // returns empty if db or collection doesn't exist
        if !client.table_exists(db, collection).unwrap() {
            return Ok(doc! {
                "cursor": doc! {
                    "firstBatch": Bson::Array(vec![]),
                    "id": Bson::Int64(0),
                    "ns": format!("{}.{}", db, collection),
                },
                "ok": Bson::Double(1.0),
            });
        }

        let filter = if doc.contains_key("filter") {
            Some(doc.get_document("filter").unwrap().clone())
        } else {
            None
        };

        let r = client.query("SELECT * FROM %table%", sp, filter, &[]);
        match r {
            Ok(rows) => {
                let mut res: Vec<Bson> = vec![];
                for row in rows.iter() {
                    let json_value: serde_json::Value = row.get(0);
                    let bson_value = json_value.from_psql_json();
                    res.push(bson_value);
                }

                Ok(doc! {
                    "cursor": doc! {
                        "firstBatch": res,
                        "id": Bson::Int64(0),
                        "ns": format!("{}.{}", db, collection),
                    },
                    "ok": Bson::Double(1.0),
                })
            }
            Err(error) => {
                log::error!("Error during find: {:?} - doc: {}", error, &doc);
                Err(CommandExecutionError::new(format!(
                    "error during find: {:?}",
                    error
                )))
            }
        }
    }
}
