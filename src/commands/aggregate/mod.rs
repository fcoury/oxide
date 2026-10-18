use crate::{
    commands::{check_options, cursor_batch_size, Handler},
    handler::{CommandExecutionError, Request},
    parser,
    pg::{integer, SqlParam, MAX_QUERY_ROWS},
    serializer::{encode_key, PostgresSerializer},
    sql::Sql,
};
use bson::{Bson, Document};
use eyre::{bail, eyre, Result};

pub struct Aggregate;
impl Handler for Aggregate {
    fn new() -> Self {
        Self
    }
    fn handle(
        &self,
        request: &Request,
        docs: &[Document],
    ) -> Result<Document, CommandExecutionError> {
        let doc = &docs[0];
        check_options(doc, &["aggregate", "pipeline", "cursor"])?;
        let sp = SqlParam::from(doc, "aggregate")?;
        let sql = compile_sql(&sp, doc.get_array("pipeline")?)?;
        let batch = cursor_batch_size(doc)?;
        let mut client = request.get_client()?;
        let documents = if sp.exists(&mut client)? {
            client.documents(&sql, None)?
        } else {
            Vec::new()
        };
        Ok(request
            .cursors
            .lock()
            .map_err(|_| CommandExecutionError::new("cursor lock unavailable".into()))?
            .first(
                format!("{}.{}", sp.db, sp.collection),
                documents,
                batch,
                false,
            )?)
    }
}

pub fn compile_sql(sp: &SqlParam, pipeline: &[Bson]) -> Result<Sql> {
    sp.validate()?;
    if bson::to_vec(&bson::doc! {"pipeline":pipeline})?.len() > 64 * 1024 {
        bail!("pipeline exceeds 64 KiB");
    }
    if pipeline.len() > 64 {
        bail!("pipeline exceeds 64 stages");
    }
    let mut sql = Sql::default();
    let mut source = format!("SELECT _jsonb FROM {sp}");
    for stage in pipeline {
        let stage = stage
            .as_document()
            .ok_or_else(|| eyre!("pipeline stage must be a document"))?;
        if stage.len() != 1 {
            bail!("pipeline stage requires exactly one operator");
        }
        let (name, value) = stage.iter().next().expect("one stage field");
        source = match name.as_str() {
            "$match" => {
                let predicate = parser::append_filter(as_doc(value)?, &mut sql)?;
                format!("SELECT _jsonb FROM ({source}) AS s WHERE {predicate}")
            }
            "$sort" => format!(
                "SELECT _jsonb FROM ({source}) AS s{}",
                crate::pg::order_by(Some(as_doc(value)?), &mut sql)?
            ),
            "$skip" | "$limit" => {
                let n = integer(value)?;
                if n < 0 || (name == "$limit" && n == 0) {
                    bail!("invalid {name} value");
                }
                format!(
                    "SELECT _jsonb FROM ({source}) AS s {} {n}",
                    if name == "$skip" { "OFFSET" } else { "LIMIT" }
                )
            }
            "$project" => {
                let projection = projection(as_doc(value)?, &mut sql)?;
                format!("SELECT {projection} AS _jsonb FROM ({source}) AS s")
            }
            "$group" => group(&source, as_doc(value)?, &mut sql)?,
            "$count" => {
                let key = value
                    .as_str()
                    .ok_or_else(|| eyre!("$count requires a string"))?;
                if key.is_empty() || key.contains('.') || key.starts_with('$') {
                    bail!("invalid $count field");
                }
                let key = sql.bind(encode_key(key), "text");
                format!("SELECT jsonb_build_object({key},to_jsonb(COUNT(*))) AS _jsonb FROM ({source}) AS s HAVING COUNT(*)>0")
            }
            _ => bail!("Unrecognized pipeline stage name: '{name}'"),
        };
    }
    sql.text = format!(
        "SELECT _jsonb FROM ({source}) AS result LIMIT {}",
        MAX_QUERY_ROWS + 1
    );
    Ok(sql)
}
fn as_doc(value: &Bson) -> Result<&Document> {
    value
        .as_document()
        .ok_or_else(|| eyre!("stage requires a document"))
}
fn field(path: &str, sql: &mut Sql) -> Result<String> {
    if path.is_empty() || path.split('.').any(str::is_empty) {
        bail!("invalid field path");
    }
    let path = path.split('.').map(encode_key).collect::<Vec<_>>();
    let path = sql.bind(serde_json::to_string(&path)?, "jsonb");
    Ok(format!(
        "_jsonb #> ARRAY(SELECT jsonb_array_elements_text({path}))"
    ))
}
fn literal(value: &Bson, sql: &mut Sql) -> String {
    sql.bind(value.clone().into_psql_json(), "jsonb")
}

/// One expression compiler is shared by projections and group accumulators.
fn expression(value: &Bson, sql: &mut Sql, depth: usize) -> Result<String> {
    if depth > 64 {
        bail!("expression exceeds 64 levels");
    }
    let result = match value {
        Bson::String(value) if value.starts_with('$') => field(&value[1..], sql)?,
        Bson::Array(values) => {
            let values = values
                .iter()
                .map(|v| expression(v, sql, depth + 1))
                .collect::<Result<Vec<_>>>()?;
            format!("jsonb_build_array({})", values.join(","))
        }
        Bson::Document(doc) if doc.keys().any(|k| k.starts_with('$')) => {
            if doc.len() != 1 {
                bail!("expression must have exactly one operator");
            }
            let (op, value) = doc.iter().next().expect("one expression field");
            match op.as_str() {
                "$literal" => literal(value, sql),
                "$add" | "$multiply" | "$subtract" | "$divide" => {
                    let values = value
                        .as_array()
                        .ok_or_else(|| eyre!("{op} requires an array"))?;
                    if values.is_empty()
                        || ((op == "$subtract" || op == "$divide") && values.len() != 2)
                    {
                        bail!("invalid operand count for {op}");
                    }
                    let inputs = values
                        .iter()
                        .map(|v| expression(v, sql, depth + 1))
                        .collect::<Result<Vec<_>>>()?;
                    let terms = inputs
                        .iter()
                        .map(|s| format!("({})", parser::numeric(&format!("({s})"))))
                        .collect::<Vec<_>>();
                    let operator = match op.as_str() {
                        "$add" => "+",
                        "$multiply" => "*",
                        "$subtract" => "-",
                        _ => "/",
                    };
                    let double = if op == "$divide" {
                        "TRUE".into()
                    } else {
                        inputs
                            .iter()
                            .map(|s| format!("COALESCE(({s}) ? '$f',FALSE)"))
                            .collect::<Vec<_>>()
                            .join(" OR ")
                    };
                    let long = inputs
                        .iter()
                        .map(|s| format!("COALESCE(({s}) ? '$i',FALSE)"))
                        .collect::<Vec<_>>()
                        .join(" OR ");
                    number_result(&terms.join(operator), &double, &long)
                }
                "$dateToString" => {
                    let doc = as_doc(value)?;
                    let format = match doc.get_str("format")? {
                        "%Y" => "YYYY",
                        "%Y-%m-%d" => "YYYY-MM-DD",
                        _ => bail!("only UTC %Y and %Y-%m-%d date formatting is supported"),
                    };
                    if doc
                        .keys()
                        .any(|k| !["format", "date"].contains(&k.as_str()))
                    {
                        bail!("only UTC %Y-%m-%d date formatting is supported");
                    }
                    let date = expression(
                        doc.get("date").ok_or_else(|| eyre!("missing date"))?,
                        sql,
                        depth + 1,
                    )?;
                    format!("to_jsonb(TO_CHAR(TO_TIMESTAMP((({date})->>'$d')::numeric/1000) AT TIME ZONE 'UTC','{format}'))")
                }
                _ => bail!("Unrecognized expression {op}"),
            }
        }
        Bson::Document(doc) => {
            let mut fields = Vec::new();
            for (key, value) in doc {
                let key = sql.bind(encode_key(key), "text");
                let value = expression(value, sql, depth + 1)?;
                // A missing path is SQL NULL; an explicit BSON null is JSON null.
                fields.push(format!("CASE WHEN ({value}) IS NULL THEN '{{}}'::jsonb ELSE jsonb_build_object({key},{value}) END"));
            }
            if fields.is_empty() {
                "'{}'::jsonb".into()
            } else {
                format!("({})", fields.join(" || "))
            }
        }
        _ => literal(value, sql),
    };
    if result.len() > 256 * 1024 {
        bail!("expression exceeds SQL complexity limit");
    }
    Ok(result)
}
fn projection(doc: &Document, sql: &mut Sql) -> Result<String> {
    if doc.is_empty() {
        bail!("projection cannot be empty");
    }
    fn include(value: &Bson) -> Option<bool> {
        match value {
            Bson::Int32(i) => Some(*i != 0),
            Bson::Int64(i) => Some(*i != 0),
            Bson::Double(f) => Some(*f != 0.0),
            Bson::Boolean(b) => Some(*b),
            _ => None,
        }
    }
    let mut mode = None;
    for (key, value) in doc {
        if key == "_id" {
            continue;
        }
        let inclusion = include(value).unwrap_or(true);
        if mode.is_some_and(|mode| mode != inclusion) {
            bail!("cannot mix inclusion and exclusion in projection");
        }
        mode = Some(inclusion);
    }
    let inclusion = mode.unwrap_or_else(|| doc.get("_id").and_then(include).unwrap_or(true));
    if !inclusion {
        let mut result = "_jsonb".to_string();
        for (key, value) in doc {
            if include(value) == Some(false) {
                let parts = key.split('.').map(encode_key).collect::<Vec<_>>();
                let path = sql.bind(serde_json::to_string(&parts)?, "jsonb");
                result = format!("({result} #- ARRAY(SELECT jsonb_array_elements_text({path})))");
            }
        }
        return Ok(result);
    }
    // Build nested output documents before rendering, preserving sibling fields.
    let mut output = Document::new();
    let mut parents = std::collections::HashSet::new();
    let mut computed = Vec::new();
    for (key, value) in doc {
        if include(value) == Some(false) {
            continue;
        }
        let value = if include(value) == Some(true) {
            let parts = key.split('.').collect::<Vec<_>>();
            for n in 1..parts.len() {
                parents.insert(parts[..n].join("."));
            }
            Bson::String(format!("${key}"))
        } else {
            computed.push(key.clone());
            value.clone()
        };
        insert_projection(&mut output, &key.split('.').collect::<Vec<_>>(), value)?;
    }
    if !doc.contains_key("_id") {
        output.insert("_id", "$_id");
    }
    parents.retain(|path| {
        !computed
            .iter()
            .any(|key| key == path || key.starts_with(&format!("{path}.")))
    });
    projection_object(&output, "", &parents, sql, 0)
}
// Dotted inclusion must not manufacture parents that were absent in the source.
fn projection_object(
    doc: &Document,
    path: &str,
    parents: &std::collections::HashSet<String>,
    sql: &mut Sql,
    depth: usize,
) -> Result<String> {
    if depth > 64 {
        bail!("projection exceeds 64 levels");
    }
    let mut fields = Vec::new();
    for (key, value) in doc {
        let child_path = if path.is_empty() {
            key.clone()
        } else {
            format!("{path}.{key}")
        };
        let mut value = match value {
            Bson::Document(d) if !d.keys().any(|k| k.starts_with('$')) => {
                projection_object(d, &child_path, parents, sql, depth + 1)?
            }
            _ => expression(value, sql, depth + 1)?,
        };
        if parents.contains(&child_path) {
            let source = field(&child_path, sql)?;
            value=format!("CASE WHEN jsonb_typeof({source})='array' THEN to_jsonb(({source})::numeric) WHEN jsonb_typeof({source}) IS DISTINCT FROM 'object' OR ({source}) ?| ARRAY['$i','$f','$d','$o','$b'] THEN NULL::jsonb ELSE {value} END");
        }
        let key = sql.bind(encode_key(key), "text");
        fields.push(format!("CASE WHEN ({value}) IS NULL THEN '{{}}'::jsonb ELSE jsonb_build_object({key},{value}) END"));
    }
    let result = if fields.is_empty() {
        "'{}'::jsonb".into()
    } else {
        format!("({})", fields.join(" || "))
    };
    if result.len() > 256 * 1024 {
        bail!("projection exceeds SQL complexity limit");
    }
    Ok(result)
}
fn insert_projection(doc: &mut Document, path: &[&str], value: Bson) -> Result<()> {
    if path.len() > 64 || path.iter().any(|p| p.is_empty() || p.starts_with('$')) {
        bail!("invalid projection path");
    }
    if path.len() == 1 {
        if doc.contains_key(path[0]) {
            bail!("conflicting projection paths");
        }
        doc.insert(path[0], value);
        return Ok(());
    }
    if !doc.contains_key(path[0]) {
        doc.insert(path[0], Document::new());
    }
    insert_projection(
        doc.get_document_mut(path[0])
            .map_err(|_| eyre!("conflicting projection paths"))?,
        &path[1..],
        value,
    )
}
fn group(source: &str, doc: &Document, sql: &mut Sql) -> Result<String> {
    let id = group_key(
        doc.get("_id").ok_or_else(|| eyre!("$group requires _id"))?,
        sql,
        0,
    )?;
    let mut fields = vec!["'_id'".into(), id.clone()];
    for (key, value) in doc {
        if key == "_id" {
            continue;
        }
        let op = as_doc(value)?;
        if op.len() != 1 {
            bail!("group accumulator requires one operator");
        }
        let (name, value) = op.iter().next().expect("one accumulator");
        if name != "$sum" && name != "$avg" {
            bail!("unsupported accumulator {name}");
        }
        let value = expression(value, sql, 0)?;
        fields.push(sql.bind(encode_key(key), "text"));
        let numeric = parser::numeric(&format!("({value})"));
        let result = if name == "$avg" {
            format!("AVG({numeric})")
        } else {
            format!("COALESCE(SUM({numeric}),0)")
        };
        let double = if name == "$avg" {
            "TRUE".into()
        } else {
            format!("COALESCE(BOOL_OR(({value}) ? '$f'),FALSE)")
        };
        let long = format!("COALESCE(BOOL_OR(({value}) ? '$i'),FALSE)");
        fields.push(number_result(&result, &double, &long));
    }
    Ok(format!(
        "SELECT jsonb_build_object({}) AS _jsonb FROM ({source}) AS s GROUP BY {id}",
        fields.join(",")
    ))
}

/// Tag computed numbers according to BSON promotion rules before JSONB storage.
fn number_result(value: &str, double: &str, long: &str) -> String {
    format!("CASE WHEN ({value}) IS NULL THEN 'null'::jsonb WHEN ({double}) OR ({value})>9223372036854775807 OR ({value})< -9223372036854775808 THEN jsonb_build_object('$f',({value})::double precision) WHEN ({long}) OR ({value})>2147483647 OR ({value})< -2147483648 THEN jsonb_build_object('$i',({value})::bigint::text) ELSE to_jsonb(({value})::integer) END")
}
fn group_key(value: &Bson, sql: &mut Sql, depth: usize) -> Result<String> {
    if depth > 64 {
        bail!("group key exceeds 64 levels");
    }
    match value {
        Bson::Document(doc) if !doc.keys().any(|k| k.starts_with('$')) => {
            let mut fields = Vec::new();
            for (key, value) in doc {
                fields.push(sql.bind(encode_key(key), "text"));
                fields.push(group_key(value, sql, depth + 1)?);
            }
            Ok(format!("jsonb_build_object({})", fields.join(",")))
        }
        Bson::Array(values) => Ok(format!(
            "jsonb_build_array({})",
            values
                .iter()
                .map(|v| group_key(v, sql, depth + 1))
                .collect::<Result<Vec<_>>>()?
                .join(",")
        )),
        _ => Ok(format!(
            "COALESCE({},'null'::jsonb)",
            crate::pg::identity_expr(&format!("({})", expression(value, sql, depth + 1)?))
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;
    #[test]
    fn empty_pipeline_and_stage_order() {
        let sp = SqlParam::new("test", "items");
        assert!(compile_sql(&sp, &[]).is_ok());
        let sql = compile_sql(
            &sp,
            &vec![doc! {"$limit":5}.into(), doc! {"$skip":2}.into()],
        )
        .unwrap();
        assert!(sql.text.contains("LIMIT 5) AS s OFFSET 2"));
    }
    #[test]
    fn expression_inputs_are_bound() {
        let sp = SqlParam::new("test", "items");
        let sql = compile_sql(
            &sp,
            &[doc! {"$project":{"name":{"$literal":"O'Reilly"}}}.into()],
        )
        .unwrap();
        assert!(!sql.text.contains("O'Reilly"));
    }
    #[test]
    fn rejects_malformed_stages() {
        let sp = SqlParam::new("test", "items");
        for stage in [
            Bson::Int32(1),
            doc! {}.into(),
            doc! {"$skip":-1}.into(),
            doc! {"$sort":{"x":"desc"}}.into(),
        ] {
            assert!(compile_sql(&sp, &[stage]).is_err());
        }
    }
}
