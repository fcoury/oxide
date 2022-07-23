#![allow(dead_code)]
use crate::commands::Handler;
use crate::handler::{CommandExecutionError, Request};
use crate::pg::SqlParam;
use crate::utils::pg_rows_to_bson;
use bson::{doc, Bson, Document};
use group_stage::process_group;
use match_stage::process_match;
use sql_statement::SqlStatement;

mod group_stage;
mod match_stage;
mod sql_statement;

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
        let pipeline = doc.get_array("pipeline").unwrap();
        let sp = SqlParam::new(db, collection);

        let mut client = request.get_client();

        let _sql = build_sql(sp, pipeline).unwrap();

        // let mut sql = SqlStatement::builder().table(sp.clone()).build();
        let res = client.raw_query("", &[]).unwrap();

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

fn build_sql(sp: SqlParam, pipeline: &Vec<Bson>) -> Result<String, CommandExecutionError> {
    let mut stages = vec![];
    for stage in pipeline {
        let stage_doc = stage.as_document().unwrap();
        let name = stage_doc.keys().next().unwrap();
        let sql = match name.as_str() {
            "$match" => process_match(stage_doc.get_document("$match").unwrap()),
            "$group" => process_group(stage_doc.get_document("$group").unwrap()),
            _ => {
                return Err(CommandExecutionError::new(format!(
                    "Unrecognized pipeline stage name: '{}'",
                    stage
                )))
            }
        };
        stages.push(sql);
    }

    println!("stages = {:#?}", stages);

    // let mut sql = SqlStatement::default();
    let mut sql: Option<SqlStatement> = None;
    for mut stage_sql in stages {
        if let Some(mut sql) = sql {
            stage_sql.add_subquery(&mut sql);
        } else {
            stage_sql.set_table(sp.clone());
        }
        sql = Some(stage_sql);
    }

    // sql.set_table(sp);
    println!("sql tree = {:#?}", sql);
    Ok(sql.unwrap().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_sql() {
        let doc = doc! {
            "pipeline": [
                doc! {
                    "$match": doc! {
                        "name": "Alice"
                    }
                },
                doc! {
                    "$group": doc! {
                        "_id": "$name",
                        "count": doc! {
                            "$sum": 1
                        }
                    }
                }
            ]
        };

        let sp = SqlParam::new("schema", "table");

        let sql = build_sql(sp, doc.get_array("pipeline").unwrap()).unwrap();
        println!("sql = {}", sql);
    }
}
