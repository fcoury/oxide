use crate::handler::CommandExecutionError;
use crate::{commands::Handler, handler::Request};
use bson::{doc, Bson, Document};
use local_ip_address::local_ip;

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
        let mut peer_addr = req.peer_addr();
        let my_local_ip = local_ip().unwrap();
        peer_addr.set_ip(my_local_ip);

        Ok(doc! {
          "ok": Bson::Double(1.0),
          "you": peer_addr.to_string(),
        })
    }
}
