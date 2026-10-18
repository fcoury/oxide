use crate::handler::{CommandExecutionError, Request};
use crate::pg::CreateTableError;
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
        docs: &[Document],
    ) -> Result<Document, CommandExecutionError> {
        crate::commands::check_options(&docs[0], &["create"])?;
        let doc = &docs[0];
        let db = doc.get_str("$db")?;
        let collection = doc.get_str("create")?;
        let sp = SqlParam::new(db, collection);
        sp.validate()?;

        let mut client = request.get_client()?;

        client.create_schema_if_not_exists(&sp.db)?;

        let r = client.create_table(sp.clone());
        match r {
            Ok(_) => Ok(doc! {
                "ok": Bson::Double(1.0),
            }),
            Err(e) => match e {
                CreateTableError::AlreadyExists(_) => Err(CommandExecutionError::new(format!(
                    "a collection '{}' already exists",
                    sp.clone()
                ))),
                CreateTableError::Other(e) => Err(CommandExecutionError::new(format!("{}", e))),
            },
        }
    }
}
