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
        let socket_addr = &req.peer_addr();

        Ok(doc! {
          "ok": Bson::Double(1.0),
          "you": socket_addr.to_string(),
        })
    }
}
