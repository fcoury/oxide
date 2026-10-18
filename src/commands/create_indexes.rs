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
        docs: &[Document],
    ) -> Result<Document, CommandExecutionError> {
        crate::commands::check_options(&docs[0], &["createIndexes", "indexes"])?;
        let doc = &docs[0];
        let db = doc.get_str("$db")?;
        let collection = doc.get_str("createIndexes")?;
        let indexes = doc.get_array("indexes")?;
        let sp = SqlParam::new(db, collection);

        let mut client = request.get_client()?;

        client.create_schema_if_not_exists(&sp.db)?;
        client.create_table_if_not_exists(&sp.db, &sp.collection)?;

        for index in indexes {
            client.create_index(
                &sp,
                index
                    .as_document()
                    .ok_or_else(|| CommandExecutionError::new("index must be a document".into()))?,
            )?;
        }

        Ok(doc! {
            "ok": Bson::Double(1.0),
        })
    }
}
