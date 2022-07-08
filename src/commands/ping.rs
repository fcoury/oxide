use crate::commands::Handler;
use crate::wire::UnknownCommandError;
use bson::{doc, Bson, Document};

pub struct Ping {}

impl Handler for Ping {
    fn new() -> Self {
        Ping {}
    }

    fn handle(&self, _msg: &Vec<Document>) -> Result<Document, UnknownCommandError> {
        Ok(doc! {
          "ok": Bson::Double(1.into())
        })
    }
}
