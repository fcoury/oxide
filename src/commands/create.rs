use crate::handler::{CommandExecutionError, Request};
use crate::pg::PgError;
use crate::{commands::Handler, pg::SqlParam};
use bson::{doc, Bson, Document};

pub struct Create {}

impl Handler for Create {
    fn new() -> Self {
        Create {}
    }

    fn handle(
        &self,
        request: &Request,
        docs: &Vec<Document>,
    ) -> Result<Document, CommandExecutionError> {
        let doc = &docs[0];
        let db = doc.get_str("$db").unwrap();
        let collection = doc.get_str("create").unwrap();
        let sp = SqlParam::new(db, collection);

        let mut client = request.get_client();

        client.create_schema_if_not_exists(&sp.db).unwrap();

        let r = client.create_table(sp.clone());
        match r {
            Ok(_) => Ok(doc! {
                "ok": Bson::Double(1.0),
            }),
            Err(e) => match e {
                PgError::AlreadyExists(_) => Err(CommandExecutionError::new(format!(
                    "a collection '{}' already exists",
                    sp.clone()
                ))),
                PgError::Other(e) => Err(CommandExecutionError::new(format!("{}", e))),
            },
        }
    }
}
