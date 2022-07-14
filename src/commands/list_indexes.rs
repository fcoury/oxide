use crate::handler::{CommandExecutionError, Request};
use crate::{commands::Handler, pg::SqlParam};
use bson::{doc, Bson, Document};

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

        return Ok(doc! {
            "cursor": doc! {
                "id": Bson::Int64(0),
                "ns": format!("{}.$cmd.listIndexes.{}", db, collection),
                "firstBatch": Bson::Array(vec![]),
            },
            "ok": Bson::Double(1.0),
        });
    }
}
