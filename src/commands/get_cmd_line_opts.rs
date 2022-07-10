use crate::commands::Handler;
use crate::handler::{CommandExecutionError, Request};
use bson::{doc, Bson, Document};

pub struct GetCmdLineOpts {}

impl Handler for GetCmdLineOpts {
    fn new() -> Self {
        GetCmdLineOpts {}
    }

    fn handle(
        &self,
        _request: &Request,
        _msg: &Vec<Document>,
    ) -> Result<Document, CommandExecutionError> {
        Ok(doc! {
            "argv": Bson::Array(vec![Bson::String("oxidedb".to_string())]),
            "parsed": doc!{},
            "ok": Bson::Double(1.into())
        })
    }
}
