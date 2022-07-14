use crate::commands::Handler;
use crate::handler::{CommandExecutionError, Request};
use bson::{doc, Bson, Document};
use sql_lexer::sanitize_string;

pub struct DropDatabase {}

impl Handler for DropDatabase {
    fn new() -> Self {
        DropDatabase {}
    }

    fn handle(
        &self,
        request: &Request,
        docs: &Vec<Document>,
    ) -> Result<Document, CommandExecutionError> {
        println!("{:?}", docs);
        let mut client = request.get_client();
        let db = sanitize_string(docs[0].get_str("$db").unwrap().to_string());
        client.drop_schema(&db).unwrap();

        Ok(doc! {
            "dropped": db,
            "ok": Bson::Double(1.0),
        })
    }
}
