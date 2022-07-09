use crate::{commands::Handler, handler::CommandExecutionError};
use bson::{doc, Bson, Document};

pub struct Ping {}

impl Handler for Ping {
    fn new() -> Self {
        Ping {}
    }

    fn handle(&self, _msg: &Vec<Document>) -> Result<Document, CommandExecutionError> {
        Ok(doc! {
          "ok": Bson::Double(1.into())
        })
    }
}
