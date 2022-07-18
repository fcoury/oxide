use crate::handler::{CommandExecutionError, Request};
use crate::{commands::Handler, pg::SqlParam};
use bson::{doc, Bson, Document};

pub struct CreateIndexes {}

impl Handler for CreateIndexes {
    fn new() -> Self {
        CreateIndexes {}
    }

    fn handle(
        &self,
        request: &Request,
        docs: &Vec<Document>,
    ) -> Result<Document, CommandExecutionError> {
        let doc = &docs[0];
        let db = doc.get_str("$db").unwrap();
        let collection = doc.get_str("createIndexes").unwrap();
        let indexes = doc.get_array("indexes").unwrap();
        let sp = SqlParam::new(db, collection);

        let mut client = request.get_client();

        client.create_schema_if_not_exists(&sp.db).unwrap();
        client
            .create_table_if_not_exists(&sp.db, &sp.collection)
            .unwrap();

        for index in indexes {
            client
                .create_index(&sp, index.as_document().unwrap())
                .unwrap();
        }

        Ok(doc! {
            "ok": Bson::Double(1.0),
        })
    }
}
