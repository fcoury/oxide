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

        let sql = build_sql(sp, pipeline).unwrap();
        log::debug!("SQL: {}", sql);

        let res = client.raw_query(&sql, &[]).unwrap();
        let res_doc = doc![
            "cursor": doc! {
                "firstBatch": pg_rows_to_bson(res),
                "id": Bson::Int64(0),
                "ns": format!("{}.{}", db, collection),
            },
            "ok": Bson::Double(1.0),
        ];

        return Ok(res_doc);
    }
}

fn build_sql(sp: SqlParam, pipeline: &Vec<Bson>) -> Result<String, CommandExecutionError> {
    let mut stages: Vec<(String, SqlStatement)> = vec![];
    for stage in pipeline {
        let stage_doc = stage.as_document().unwrap();
        let name = stage_doc.keys().next().unwrap();
        match name.as_str() {
            "$match" => {
                let sql = process_match(stage_doc.get_document("$match").unwrap());
                stages.push((name.to_string(), sql));
            }
            "$group" => {
                stages.push((
                    name.to_string(),
                    process_group(stage_doc.get_document("$group").unwrap()),
                ));
                let wrap_sql = SqlStatement::builder()
                    .field("row_to_json(s_wrap)::jsonb AS _jsonb")
                    .build();
                stages.push(("$wrap".to_string(), wrap_sql));
            }
            _ => {
                return Err(CommandExecutionError::new(format!(
                    "Unrecognized pipeline stage name: '{}'",
                    stage
                )))
            }
        };
    }

    let mut sql: Option<SqlStatement> = None;
    for (name, mut stage_sql) in stages {
        if stage_sql.from.is_none() {
            if let Some(mut sql) = sql {
                let alias = format!("s_{}", name.strip_prefix("$").unwrap());
                stage_sql.add_subquery_with_alias(&mut sql, &alias);
            } else {
                stage_sql.set_table(sp.clone());
            }
        }
        sql = Some(stage_sql);
    }

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
        assert_eq!(
            sql,
            r#"SELECT row_to_json(s_wrap)::jsonb AS _jsonb FROM (SELECT _jsonb->'name' AS _id, SUM(1) AS count FROM (SELECT * FROM "schema"."table" WHERE _jsonb->'name' = '"Alice"') AS s_group GROUP BY _jsonb->'name') AS s_wrap"#
        );
    }
}
