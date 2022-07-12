use crate::handler::{CommandExecutionError, Request};
use crate::{commands::Handler, pg::SqlParam};
use bson::{doc, Bson, Document};

pub struct Drop {}

impl Handler for Drop {
    fn new() -> Self {
        Drop {}
    }

    fn handle(
        &self,
        request: &Request,
        docs: &Vec<Document>,
    ) -> Result<Document, CommandExecutionError> {
        let mut client = request.get_client();
        let sp = SqlParam::from(&docs[0], "drop");
        client.drop_table(&sp).unwrap();

        Ok(doc! {
            "nIndexesWas": Bson::Int32(1), // TODO
            "ns": Bson::String(sp.to_string()),
            "ok": Bson::Double(1.0),
        })
    }
}
