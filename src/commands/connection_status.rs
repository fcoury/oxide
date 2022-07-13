use crate::commands::Handler;
use crate::handler::{CommandExecutionError, Request};
use bson::{doc, Bson, Document};

pub struct ConnectionStatus {}

impl Handler for ConnectionStatus {
    fn new() -> Self {
        ConnectionStatus {}
    }

    fn handle(
        &self,
        _request: &Request,
        _msg: &Vec<Document>,
    ) -> Result<Document, CommandExecutionError> {
        Ok(doc! {
          "authInfo": {
            "authenticatedUsers": [],
            "authenticatedUserRoles": [],
            "authenticatedUserPrivileges": [],
          },
          "ok": Bson::Double(1.into())
        })
    }
}
