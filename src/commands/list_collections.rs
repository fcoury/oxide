use crate::handler::Request;
use crate::{commands::Handler, handler::CommandExecutionError};
use bson::{bson, doc, Bson, Document};

pub struct ListCollections {}

impl Handler for ListCollections {
    fn new() -> Self {
        ListCollections {}
    }

    fn handle(
        &self,
        request: &Request,
        docs: &Vec<Document>,
    ) -> Result<Document, CommandExecutionError> {
        let doc = &docs[0];
        let mut client = request.get_client();
        let db = doc.get_str("$db").unwrap();
        let tables = client.get_tables(db);
        let collections = tables
            .into_iter()
            .map(|t| {
                bson!({
                    "name": t,
                    "type": "collection",
                    "options": {},
                    "info": {
                        "readOnly": false,
                    },
                    "idIndex": {},
                })
            })
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
