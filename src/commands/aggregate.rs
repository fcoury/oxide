#![allow(dead_code)]
use crate::handler::{CommandExecutionError, Request};
use crate::parser::parse;
use crate::utils::{
    collapse_fields, convert_if_numeric, expand_fields, field_to_jsonb, pg_rows_to_bson,
};
use crate::{commands::Handler, pg::SqlParam};
use bson::{doc, Bson, Document};

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

fn process_match(sp: &SqlParam, doc: &Document) -> String {
    let mut sql = format!("SELECT _jsonb FROM {}", sp.sanitize());

    let filter_doc = expand_fields(&doc).unwrap();
    let filter = parse(filter_doc);
    if filter != "" {
        sql = format!("{} WHERE {}", sql, filter);
    }

    sql
}

fn process_group(sp: &SqlParam, doc: &Document) -> String {
    let mut sql = "SELECT".to_owned();
    let mut doc = doc.clone();

    let mut groups = vec![];
    let mut fields = vec![];
    if doc.contains_key("_id") {
        let field = doc.remove("_id").unwrap();
        let field = field.as_str().unwrap();
        if let Some(field) = field.strip_prefix("$") {
            let field = field_to_jsonb(field);
            groups.push(format!("{}", field));
            fields.push(format!("{} AS _id", field));
        } else {
            todo!("group by field: {}", field);
        }
    }

    let doc = collapse_fields(&doc);
    for (key, value) in doc.iter() {
        if key.ends_with("$sum") {
            // name of the resulting field - AS xxxx
            let key = key.strip_suffix(".$sum").unwrap();

            // what to sum SUM(xxxx)
            let value = if let Some(str_value) = value.as_str() {
                // if it's a string starting with $ we take it as a field name
                if let Some(field_name) = str_value.strip_prefix("$") {
                    convert_if_numeric(&field_to_jsonb(field_name))
                } else {
                    // FIXME we can't do anything yet for summing other types
                    todo!("unsupported sum value without $: {}", str_value);
                }
            } else {
                // if it's not a string, we take its contents as is
                value.to_string()
            };

            fields.push(format!("SUM({}) AS {}", value, key));
        }
    }

    let fields = if fields.is_empty() {
        "*".to_string()
    } else {
        fields.join(", ")
    };

    sql = format!("{} {} FROM {}", sql, fields, sp.sanitize());

    if !groups.is_empty() {
        let group_str = format!("GROUP BY {}", groups.join(", "));
        sql = format!("{} {}", sql, group_str);
    }

    sql = format!("SELECT row_to_json(t) FROM ({}) t", sql);
    sql.to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_match() {
        let sp = SqlParam::new("test", "test");
        let doc = doc! { "age": { "$gt": 20 } };
        let sql = process_match(&sp, &doc);
        assert_eq!(
            sql,
            r#"SELECT _jsonb FROM "test"."test" WHERE (jsonb_typeof(_jsonb->'age') = 'number' OR jsonb_typeof(_jsonb->'age'->'$f') = 'number') AND (CASE WHEN (_jsonb->'age' ? '$f') THEN (_jsonb->'age'->>'$f')::numeric ELSE (_jsonb->'age')::numeric END) > '20'"#
        );
    }

    #[test]
    fn test_process_group_with_sum_int() {
        let sp = SqlParam::new("test", "test");
        let doc = doc! { "_id": "$field", "count": { "$sum": 1 } };
        let sql = process_group(&sp, &doc);
        assert_eq!(
            sql,
            r#"SELECT row_to_json(t) FROM (SELECT _jsonb->'field' AS _id, SUM(1) AS count FROM "test"."test" GROUP BY _jsonb->'field') t"#
        );
    }

    #[test]
    fn test_process_group_with_sum_field() {
        let sp = SqlParam::new("test", "test");
        let doc = doc! { "_id": "$field", "qty": { "$sum": "$qty" } };
        let sql = process_group(&sp, &doc);
        assert_eq!(
            sql,
            r#"SELECT row_to_json(t) FROM (SELECT _jsonb->'field' AS _id, SUM((CASE WHEN (_jsonb->'qty' ? '$f') THEN (_jsonb->'qty'->>'$f')::numeric ELSE (_jsonb->'qty')::numeric END)) AS qty FROM "test"."test" GROUP BY _jsonb->'field') t"#
        );
    }
}
