#![allow(dead_code)]
use crate::handler::{CommandExecutionError, Request};
use crate::utils::pg_rows_to_bson;
use crate::{commands::Handler, pg::SqlParam};
use bson::{doc, Bson, Document};
use group_stage::process_group;
use match_stage::process_match;

mod group_stage;
mod match_stage;

pub struct Aggregate {}

impl Handler for Aggregate {
    fn new() -> Self {
        Aggregate {}
    }

    fn handle(
        &self,
        request: &Request,
        docs: &Vec<Document>,
    ) -> Result<Document, CommandExecutionError> {
        let doc = &docs[0];
        let db = doc.get_str("$db").unwrap();
        let collection = doc.get_str("aggregate").unwrap();
        let sp = SqlParam::new(db, collection);
        let pipeline = doc.get_array("pipeline").unwrap();

        let mut client = request.get_client();

        let mut stages = vec![];
        for stage in pipeline {
            let stage_doc = stage.as_document().unwrap();
            let name = stage_doc.keys().next().unwrap();
            let sql = match name.as_str() {
                "$match" => process_match(&sp, stage_doc.get_document("$match").unwrap()),
                "$group" => process_group(&sp, stage_doc.get_document("$group").unwrap()),
                _ => {
                    return Err(CommandExecutionError::new(format!(
                        "Unrecognized pipeline stage name: '{}'",
                        stage
                    )))
                }
            };
            stages.push((name.to_string(), sql));
        }

        let mut sql = "".to_string();
        for stages in stages {
            let val = format!("{}{}", sql, stages.1);
            sql = val;
        }

        let res = client.raw_query(sql.as_str(), &[]).unwrap();

        return Ok(doc![
            "cursor": doc! {
                "firstBatch": pg_rows_to_bson(res),
                "id": Bson::Int64(0),
                "ns": format!("{}.{}", db, collection),
            },
            "ok": Bson::Double(1.0),
        ]);
    }
}
