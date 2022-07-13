use crate::handler::CommandExecutionError;
use crate::{commands::Handler, handler::Request};
use bson::{doc, Bson, Document};

pub struct WhatsMyUri {}

impl Handler for WhatsMyUri {
    fn new() -> Self {
        WhatsMyUri {}
    }

    fn handle(
        &self,
        req: &Request,
        _msg: &Vec<Document>,
    ) -> Result<Document, CommandExecutionError> {
        Ok(doc! {
          "ok": Bson::Double(1.0),
          "you": req.peer_addr().to_string(),
        })
    }
}
