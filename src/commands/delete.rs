use crate::handler::{CommandExecutionError, Request};
use crate::{commands::Handler, pg::SqlParam};
use bson::{doc, Bson, Document};

pub struct Delete {}

impl Handler for Delete {
    fn new() -> Self {
        Delete {}
    }

    fn handle(
        &self,
        request: &Request,
        docs: &Vec<Document>,
    ) -> Result<Document, CommandExecutionError> {
        let doc = &docs[0];
        let db = doc.get_str("$db").unwrap();
        let collection = doc.get_str("delete").unwrap();
        let deletes = doc.get_array("deletes").unwrap();
        let sp = SqlParam::new(db, collection);
        let mut client = request.get_client();

        let exists = sp.exists(&mut client);

        match exists {
            Ok(exists) => {
                if !exists {
                    return Ok(doc! {
                        "n": Bson::Int64(0),
                        "ok": Bson::Double(1.0),
                    });
                }
            }
            Err(e) => {
                return Err(CommandExecutionError::new(e.to_string()));
            }
        };

        if deletes.len() > 1 {
            return Err(CommandExecutionError::new(
                "Only one delete operation is supported".to_string(),
            ));
        }

        let delete_doc = deletes[0].as_document().unwrap();
        let filter = delete_doc.get_document("q").unwrap();
        let limit: Option<i32> = delete_doc.get_i32("limit").ok();

        let n = client.delete(&sp, Some(filter), limit).unwrap();

        Ok(doc! {
            "n": Bson::Int64(n as i64),
            "ok": Bson::Double(1.0),
        })
    }
}
