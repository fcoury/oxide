use crate::handler::{CommandExecutionError, Request};
use crate::{commands::Handler, pg::SqlParam};
use bson::{doc, Document};

pub struct FindAndModify {}

impl Handler for FindAndModify {
    fn new() -> Self {
        FindAndModify {}
    }

    fn handle(
        &self,
        _request: &Request,
        docs: &Vec<Document>,
    ) -> Result<Document, CommandExecutionError> {
        let doc = &docs[0];
        let db = doc.get_str("$db").unwrap();
        let collection = doc.get_str("findAndModify").unwrap();
        let _sp = SqlParam::new(db, collection);
        let query = doc.get_document("query").unwrap();
        let update = doc.get_document("update").unwrap();
        println!("doc = {:?}", doc);
        println!("query = {:?}", query);
        println!("update = {:?}", update);

        Ok(doc! {})
    }
}
