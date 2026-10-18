use crate::commands::Handler;
use crate::handler::{CommandExecutionError, Request};
use bson::{doc, Bson, Document};

pub struct DropDatabase {}

impl Handler for DropDatabase {
    fn new() -> Self {
        DropDatabase {}
    }

    fn handle(
        &self,
        request: &Request,
        docs: &[Document],
    ) -> Result<Document, CommandExecutionError> {
        crate::commands::check_options(&docs[0], &["dropDatabase"])?;
        let mut client = request.get_client()?;
        let db = docs[0].get_str("$db")?.to_string();
        client.drop_schema(&db)?;

        Ok(doc! {
            "dropped": db,
            "ok": Bson::Double(1.0),
        })
    }
}
