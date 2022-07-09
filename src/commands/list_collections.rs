use crate::handler::Request;
use crate::pg::PgDb;
use crate::{commands::Handler, handler::CommandExecutionError};
use bson::{bson, doc, Bson, Document};

pub struct ListCollections {}

impl Handler for ListCollections {
    fn new() -> Self {
        ListCollections {}
    }

    fn handle(
        &self,
        _request: &Request,
        docs: &Vec<Document>,
    ) -> Result<Document, CommandExecutionError> {
        let doc = &docs[0];
        let mut client = PgDb::new();
        let db = doc.get_str("$db").unwrap();
        let tables = client.get_tables(db);
        let collections = tables
            .into_iter()
            .map(|t| bson!({"name": t, "type": "collection"}))
            .collect();

        Ok(doc! {
            "cursor": doc! {
                "id": Bson::Int64(0),
                "ns": Bson::String(format!("{}.$cmd.listCollections", db)),
                "firstBatch": Bson::Array(collections),
            },
            "ok": Bson::Double(1.0),
        })
    }
}
