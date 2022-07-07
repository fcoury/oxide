use crate::commands::Handler;
use crate::wire::UnknownCommandError;
use bson::{doc, Bson, Document};

pub struct Insert {}

impl Handler for Insert {
    fn new() -> Self {
        Insert {}
    }

    fn handle(&self, _msg: Document) -> Result<Document, UnknownCommandError> {
        Ok(doc! {
          "ok": Bson::Double(1.0),
          "n": Bson::Int64(1),
          "lastErrorObject": doc! {
            "updatedExisting": Bson::Boolean(false),
            "n": Bson::Int64(1),
            "ok": Bson::Double(1.0),
          },
        })
    }
}
