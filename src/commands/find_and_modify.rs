use crate::handler::{CommandExecutionError, Request};
use crate::parser::parse_update;
use crate::pg::UpdateResult;
use crate::{commands::Handler, pg::SqlParam};
use bson::{doc, Bson, Document};

pub struct FindAndModify {}

impl Handler for FindAndModify {
    fn new() -> Self {
        FindAndModify {}
    }

    fn handle(
        &self,
        request: &Request,
        docs: &Vec<Document>,
    ) -> Result<Document, CommandExecutionError> {
        let doc = &docs[0];
        let db = doc.get_str("$db").unwrap();
        let collection = doc.get_str("findAndModify").unwrap();
        let sp = SqlParam::new(db, collection);
        let query = doc.get_document("query").unwrap();
        let update_doc = parse_update(doc.get_document("update").unwrap());
        let upsert = doc.get_bool("upsert").unwrap_or(false);

        let mut client = request.get_client();
        client.create_table_if_not_exists(db, collection).unwrap();

        let res = client
            .update(&sp, Some(query), update_doc.unwrap(), upsert, false, true)
            .unwrap();

        match res {
            UpdateResult::Count(total) => Ok(doc! {
                "n": Bson::Int64(total.try_into().unwrap()),
                "nModified": Bson::Int64(total as i64),
                "ok": Bson::Double(1.0),
            }),
            UpdateResult::Document(value) => Ok(doc! {
                "n": Bson::Int64(1),
                "value": value,
                "ok": Bson::Double(1.0),
            }),
        }
    }
}
