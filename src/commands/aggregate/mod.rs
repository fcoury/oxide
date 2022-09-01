#![allow(dead_code)]
use crate::commands::Handler;
use crate::handler::{CommandExecutionError, Request};
use crate::pg::SqlParam;
use crate::utils::{field_to_jsonb, pg_rows_to_bson};
use bson::{doc, Bson, Document};
use group_stage::process_group;
use match_stage::process_match;
use project_stage::process_project;
use sql_statement::SqlStatement;

use self::count_stage::process_count;

mod count_stage;
mod group_id;
mod group_stage;
mod match_stage;
mod project_stage;
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

        let sql = build_sql(&sp, pipeline);
        match sql {
            Ok(sql) => {
                log::debug!("SQL: {}", sql);
                client.trace(Some(doc.clone()), &sql);

                match client.raw_query(&sql, &[]) {
                    Ok(rows) => {
                        let res_doc = doc![
                            "cursor": doc! {
                                "firstBatch": pg_rows_to_bson(rows),
                                "id": Bson::Int64(0),
                                "ns": format!("{}.{}", db, collection),
                            },
                            "ok": Bson::Double(1.0),
                        ];

                        return Ok(res_doc);
                    }
                    Err(e) => Err(CommandExecutionError::new(e.to_string())),
                }
            }
            Err(e) => Err(e),
        }
    }
}

pub fn build_sql(sp: &SqlParam, pipeline: &Vec<Bson>) -> Result<String, CommandExecutionError> {
    let mut stages: Vec<(String, SqlStatement)> = vec![];
    for stage in pipeline {
        let stage_doc = stage.as_document().unwrap();
        let name = stage_doc.keys().next().unwrap();
        match name.as_str() {
            "$match" => {
                // adds the result of the match
                match process_match(stage_doc.get_document("$match").unwrap()) {
                    Ok(sql) => stages.push((name.to_string(), sql)),
                    Err(err) => return Err(CommandExecutionError::new(err.to_string())),
                }
            }
            "$group" => {
                // adds the group stage
                match process_group(stage_doc.get_document("$group").unwrap()) {
                    Ok(sql) => stages.push((name.to_string(), sql)),
                    Err(err) => return Err(CommandExecutionError::new(err.to_string())),
                }

                // and wraps it into a jsonb object
                let wrap_sql = SqlStatement::builder()
                    .field("row_to_json(s_wrap)::jsonb AS _jsonb")
                    .build();
                stages.push(("$wrap".to_string(), wrap_sql));
            }
            "$sort" => {
                // if there are no stages, add one
                if stages.len() < 1 {
                    stages.push((name.to_string(), SqlStatement::new()));
                }

                // adds ORDER BY to the last stage so far
                if let Some(last_stage) = stages.last_mut() {
                    for (field, value) in stage_doc.get_document("$sort").unwrap() {
                        let field = if last_stage.0 == "$wrap" {
                            format!("row_to_json(s_wrap)::jsonb->'{}'", field)
                        } else {
                            field_to_jsonb(field)
                        };
                        let asc = match value {
                            Bson::Int32(i) => *i > 0,
                            Bson::Int64(i) => *i > 0,
                            t => unimplemented!("Missing $sort handling for {:?}", t),
                        };
                        last_stage.1.add_order(&field, asc);
                    }
                }
            }
            "$project" => match process_project(stage_doc.get_document("$project").unwrap()) {
                Ok(sql) => {
                    stages.push((name.to_string(), sql));
                }
                Err(e) => {
                    return Err(CommandExecutionError::new(e.message));
                }
            },
            "$count" => match process_count(stage_doc.get_str("$count").unwrap()) {
                Ok(sql) => {
                    stages.push((name.to_string(), sql));
                }
                Err(e) => {
                    return Err(CommandExecutionError::new(e.to_string()));
                }
            },
            "$skip" => {
                // if there are no stages, add one
                if stages.len() < 1 {
                    stages.push((name.to_string(), SqlStatement::new()));
                }

                // adds offset to the last stage so far
                if let Some(last_stage) = stages.last_mut() {
                    // FIXME: the documentation states i64 but we're using i32 here
                    //        https://www.mongodb.com/docs/manual/reference/operator/aggregation/skip/
                    last_stage.1.offset =
                        Some(stage_doc.get_i32("$skip").unwrap().try_into().unwrap());
                }
            }
            "$limit" => {
                // if there are no stages, add one
                if stages.len() < 1 {
                    stages.push((name.to_string(), SqlStatement::new()));
                }

                // adds offset to the last stage so far
                if let Some(last_stage) = stages.last_mut() {
                    // FIXME: the documentation states i64 but we're using i32 here
                    //        https://www.mongodb.com/docs/manual/reference/operator/aggregation/skip/
                    last_stage.1.limit =
                        Some(stage_doc.get_i32("$limit").unwrap().try_into().unwrap());
                }
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
        let sql = build_sql(&sp, doc.get_array("pipeline").unwrap()).unwrap();
        assert_eq!(
            sql,
            r#"SELECT row_to_json(s_wrap)::jsonb AS _jsonb FROM (SELECT _jsonb->'name' AS _id, SUM(1) AS count FROM (SELECT * FROM "schema"."table" WHERE _jsonb->'name' = '"Alice"') AS s_group GROUP BY _id) AS s_wrap"#
        );
    }

    #[test]
    fn test_build_sql_with_date() {
        let doc = doc! {
            "pipeline": [
                doc! {
                    "$group": {
                        "_id": {
                            "$dateToString": {
                                "format": "%Y",
                                "date": "$date"
                            }
                        },
                        "count": {
                            "$sum": 1
                        }
                    }
                }
            ]
        };

        let sp = SqlParam::new("schema", "table");
        let sql = build_sql(&sp, doc.get_array("pipeline").unwrap()).unwrap();
        assert_eq!(
            sql,
            r#"SELECT row_to_json(s_wrap)::jsonb AS _jsonb FROM (SELECT TO_CHAR(TO_TIMESTAMP((_jsonb->'date'->>'$d')::numeric / 1000), 'YYYY-MM-DD') AS _id, SUM(1) AS count FROM "schema"."table" GROUP BY _id) AS s_wrap"#
        );
    }

    #[test]
    fn test_build_sql_with_multiply() {
        let doc = doc! {
            "pipeline": [
                doc! {
                    "$group": {
                        "_id": "$item",
                        "total_sum": {
                            "$sum": {
                                "$multiply": [
                                    "$quantity",
                                    "$price"
                                ]
                            }
                        }
                    }
                }
            ]
        };

        let sp = SqlParam::new("schema", "table");
        let sql = build_sql(&sp, doc.get_array("pipeline").unwrap()).unwrap();
        assert_eq!(
            sql,
            r#"SELECT row_to_json(s_wrap)::jsonb AS _jsonb FROM (SELECT _jsonb->'item' AS _id, SUM(CASE WHEN (_jsonb->'quantity' ? '$f') THEN (_jsonb->'quantity'->>'$f')::numeric ELSE (_jsonb->'quantity')::numeric END * CASE WHEN (_jsonb->'price' ? '$f') THEN (_jsonb->'price'->>'$f')::numeric ELSE (_jsonb->'price')::numeric END) AS total_sum FROM "schema"."table" GROUP BY _id) AS s_wrap"#
        );
    }
}
