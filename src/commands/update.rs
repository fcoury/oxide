#![allow(dead_code)]
use crate::handler::{CommandExecutionError, Request};
use crate::parser::parse_update;
use crate::{commands::Handler, pg::SqlParam, pg::UpdateResult};
use bson::{doc, Bson, Document};

pub struct Update {}

impl Handler for Update {
    fn new() -> Self {
        Update {}
    }

    fn handle(
        &self,
        request: &Request,
        docs: &Vec<Document>,
    ) -> Result<Document, CommandExecutionError> {
        let doc = &docs[0];
        let db = doc.get_str("$db").unwrap();
        let collection = doc.get_str("update").unwrap();
        let updates = doc.get_array("updates").unwrap();
        let sp = SqlParam::new(db, collection);

        let mut client = request.get_client();
        client.create_table_if_not_exists(db, collection).unwrap();

        let mut n = 0;
        for update in updates {
            let doc = update.as_document().unwrap();
            let q = doc.get_document("q").unwrap();
            let update_doc = parse_update(doc.get_document("u").unwrap());
            let upsert = doc.get_bool("upsert").unwrap_or(false);
            let multi = doc.get_bool("multi").unwrap_or(false);

            if update_doc.is_err() {
                return Err(CommandExecutionError::new(format!("{:?}", update_doc)));
            }

            let result = client
                .update(
                    &sp,
                    Some(q),
                    None,
                    update_doc.unwrap(),
                    upsert,
                    multi,
                    false,
                )
                .unwrap();

            match result {
                UpdateResult::Count(total) => n += total,
                UpdateResult::Document(_) => n += 1,
            }
        }

        Ok(doc! {
            "n": Bson::Int64(n.try_into().unwrap()),
            "nModified": Bson::Int64(n as i64),
            "ok": Bson::Double(1.0),
        })
    }
}
