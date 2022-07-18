use crate::handler::{CommandExecutionError, Request};
use crate::{commands::Handler, pg::SqlParam};
use bson::{doc, Bson, Document};
use regex::Regex;

pub struct ListIndexes {}

impl Handler for ListIndexes {
    fn new() -> Self {
        ListIndexes {}
    }

    fn handle(
        &self,
        request: &Request,
        docs: &Vec<Document>,
    ) -> Result<Document, CommandExecutionError> {
        let doc = &docs[0];
        let db = doc.get_str("$db").unwrap();
        let collection = doc.get_str("listIndexes").unwrap();
        let sp = SqlParam::new(db, collection);

        let mut client = request.get_client();
        let tables = client.get_tables(&sp.db);

        if !tables.contains(&collection.to_string()) {
            return Err(CommandExecutionError::new(format!(
                "Collection '{}' doesn't exist",
                collection
            )));
        }

        let mut indexes: Vec<Bson> = vec![];
        let regex = Regex::new(r"_jsonb\s->\s'(.*?)'").unwrap();
        for table in tables {
            for row in &mut client.get_table_indexes(&sp.db, &table).unwrap() {
                let name: String = row.get("indexname");
                let def: String = row.get("indexdef");

                let mut keys: Document = doc! {};
                for cap in regex.captures_iter(def.as_str()) {
                    keys.insert(cap[1].to_string(), 1);
                }

                indexes.push(Bson::Document(doc! {
                    "v": 2,
                    "key": keys,
                    "name": name,
                }));
            }
        }

        return Ok(doc! {
            "cursor": doc! {
                "id": Bson::Int64(0),
                "ns": format!("{}.$cmd.listIndexes.{}", db, collection),
                "firstBatch": Bson::Array(indexes),
            },
            "ok": Bson::Double(1.0),
        });
    }
}
