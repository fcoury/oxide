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
        if doc
            .get_array("pipeline")?
            .first()
            .and_then(Bson::as_document)
            .is_some_and(|stage| stage.contains_key("$changeStream"))
        {
            return crate::change_stream::open(request, doc);
        }
        check_options(doc, &["aggregate", "pipeline", "cursor"])?;
        let sp = SqlParam::from(doc, "aggregate")?;
        let batch = cursor_batch_size(doc)?;
        let mut client = request.get_client()?;
        validate_pipeline(doc.get_array("pipeline")?)?;
        let mut sql = Sql::default();
        let source = compile_pipeline(
            &sp,
            doc.get_array("pipeline")?,
            &mut sql,
            &mut |sp| client.table_exists(&sp.db, &sp.collection),
            None,
            0,
        )?;
        sql.text = format!(
            "SELECT _jsonb FROM ({source}) AS result LIMIT {}",
            MAX_QUERY_ROWS + 1
        );
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

pub fn compile_stream_pipeline(
    sp: &SqlParam,
    pipeline: &[Bson],
    sql: &mut Sql,
    initial: String,
) -> Result<String> {
    validate_pipeline(pipeline)?;
    compile_pipeline(
        sp,
        pipeline,
        sql,
        &mut |_| bail!("change stream lookups are not supported"),
        Some(initial),
        0,
    )
}

pub fn compile_sql(sp: &SqlParam, pipeline: &[Bson]) -> Result<Sql> {
    validate_pipeline(pipeline)?;
    let mut sql = Sql::default();
    let source = compile_pipeline(sp, pipeline, &mut sql, &mut |_| Ok(true), None, 0)?;
    sql.text = format!(
        "SELECT _jsonb FROM ({source}) AS result LIMIT {}",
        MAX_QUERY_ROWS + 1
    );
    Ok(sql)
}

fn validate_pipeline(pipeline: &[Bson]) -> Result<()> {
    if bson::to_vec(&bson::doc! {"pipeline":pipeline})?.len() > 64 * 1024 {
        bail!("pipeline exceeds 64 KiB");
    }
    if pipeline.len() > 256 {
        bail!("pipeline exceeds 256 stages");
    }
    Ok(())
}

fn compile_pipeline(
    sp: &SqlParam,
    pipeline: &[Bson],
    sql: &mut Sql,
    exists: &mut dyn FnMut(&SqlParam) -> Result<bool>,
    initial: Option<String>,
    depth: usize,
) -> Result<String> {
    sp.validate()?;
    if depth > 16 || pipeline.len() > 256 {
        bail!("lookup pipeline exceeds nesting or stage limit");
    }
    let mut source = if let Some(initial) = initial {
        initial
    } else if exists(sp)? {
        format!("SELECT _jsonb FROM {sp}")
    } else {
        "SELECT NULL::jsonb AS _jsonb WHERE FALSE".into()
    };
    let mut stages = Vec::new();
    for (stage_index, stage) in pipeline.iter().enumerate() {
        let stage = stage
            .as_document()
            .ok_or_else(|| eyre!("pipeline stage must be a document"))?;
        if stage.len() != 1 {
            bail!("pipeline stage requires exactly one operator");
        }
        let (name, value) = stage.iter().next().expect("one stage field");
        source = match name.as_str() {
            "$match" => {
                let predicate = parser::append_filter(as_doc(value)?, sql)?;
                format!("SELECT _jsonb FROM ({source}) AS s WHERE {predicate}")
            }
            "$sort" => format!(
                "SELECT _jsonb FROM ({source}) AS s{}",
                crate::pg::order_by(Some(as_doc(value)?), sql)?
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
                let projection = projection(as_doc(value)?, sql)?;
                format!("SELECT {projection} AS _jsonb FROM ({source}) AS s")
            }
            "$set" | "$addFields" => {
                let fields = as_doc(value)?;
                if fields
                    .keys()
                    .any(|key| key.is_empty() || key.contains('.') || key.starts_with('$'))
                {
                    bail!("{name} currently requires top-level output fields");
                }
                let keys = sql.bind(
                    serde_json::to_string(
                        &fields.keys().map(|key| encode_key(key)).collect::<Vec<_>>(),
                    )?,
                    "jsonb",
                );
                let computed = expression(value, sql, 0)?;
                // All expressions read the original document. A missing input
                // removes an existing output key instead of retaining its value.
                format!("SELECT (_jsonb - ARRAY(SELECT jsonb_array_elements_text({keys}))) || ({computed}) AS _jsonb FROM ({source}) AS s")
            }
            "$unset" => {
                let keys = match value {
                    Bson::String(key) => vec![key.clone()],
                    Bson::Array(keys) if !keys.is_empty() => keys
                        .iter()
                        .map(|key| {
                            key.as_str()
                                .map(str::to_owned)
                                .ok_or_else(|| eyre!("$unset requires field names"))
                        })
                        .collect::<Result<Vec<_>>>()?,
                    _ => bail!("$unset requires a field name or nonempty array of field names"),
                };
                let excluded = keys.into_iter().map(|key| (key, Bson::Int32(0))).collect();
                let result = projection(&excluded, sql)?;
                format!("SELECT {result} AS _jsonb FROM ({source}) AS s")
            }
            "$lookup" => {
                let lookup = as_doc(value)?;
                if lookup.keys().any(|key| {
                    !["from", "localField", "foreignField", "as", "pipeline"]
                        .contains(&key.as_str())
                }) {
                    bail!("unsupported $lookup option");
                }
                let foreign = SqlParam::new(&sp.db, lookup.get_str("from")?);
                foreign.validate()?;
                let output = lookup.get_str("as")?;
                if output.is_empty() || output.contains('.') || output.starts_with('$') {
                    bail!("$lookup currently requires a top-level output field");
                }
                let alias = format!("lookup_{depth}_{stage_index}");
                let mut foreign_source = if exists(&foreign)? {
                    format!("SELECT _jsonb FROM {foreign}")
                } else {
                    "SELECT NULL::jsonb AS _jsonb WHERE FALSE".into()
                };
                match (lookup.get("localField"), lookup.get("foreignField")) {
                    (Some(Bson::String(local)), Some(Bson::String(remote))) => {
                        let local = field_from(&format!("{alias}.document"), local, sql, 0)?;
                        let remote = field(remote, sql)?;
                        let candidates = |value: &str| {
                            format!("jsonb_array_elements(CASE WHEN jsonb_typeof({value})='array' THEN {value} ELSE jsonb_build_array(COALESCE({value},'null'::jsonb)) END)")
                        };
                        let equal = format!(
                            "{} = {}",
                            crate::pg::identity_expr("local_value"),
                            crate::pg::identity_expr("remote_value")
                        );
                        foreign_source = format!("SELECT _jsonb FROM ({foreign_source}) AS foreign_rows WHERE EXISTS (SELECT 1 FROM {} AS l(local_value), {} AS r(remote_value) WHERE {equal})", candidates(&local), candidates(&remote));
                    }
                    (None, None) => {}
                    _ => bail!("$lookup requires both localField and foreignField as strings"),
                }
                let pipeline = match lookup.get("pipeline") {
                    Some(Bson::Array(pipeline)) => pipeline.as_slice(),
                    None => &[],
                    _ => bail!("$lookup pipeline must be an array"),
                };
                let rows = compile_pipeline(
                    &foreign,
                    pipeline,
                    sql,
                    exists,
                    Some(foreign_source),
                    depth + 1,
                )?;
                let key = sql.bind(encode_key(output), "text");
                format!("SELECT {alias}.document || jsonb_build_object({key}, COALESCE((SELECT jsonb_agg(_jsonb) FROM ({rows}) AS joined), '[]'::jsonb)) AS _jsonb FROM (SELECT _jsonb AS document FROM ({source}) AS input) AS {alias}")
            }
            "$unwind" => {
                let (path, preserve, index) = match value {
                    Bson::String(path) => (path.as_str(), false, None),
                    Bson::Document(options) => {
                        if options.keys().any(|key| {
                            !["path", "preserveNullAndEmptyArrays", "includeArrayIndex"]
                                .contains(&key.as_str())
                        }) {
                            bail!("unsupported $unwind option");
                        }
                        (
                            options.get_str("path")?,
                            crate::pg::optional_bool(options, "preserveNullAndEmptyArrays", false)?,
                            options
                                .get("includeArrayIndex")
                                .map(|index| {
                                    index
                                        .as_str()
                                        .ok_or_else(|| eyre!("includeArrayIndex must be a string"))
                                })
                                .transpose()?,
                        )
                    }
                    _ => bail!("$unwind requires a path or options document"),
                };
                let path = path
                    .strip_prefix('$')
                    .ok_or_else(|| eyre!("$unwind path must start with $"))?;
                if path.is_empty() || path.contains('.') || path.starts_with('$') {
                    bail!("$unwind currently requires a top-level path");
                }
                let key = sql.bind(encode_key(path), "text");
                let array = format!("CASE WHEN jsonb_typeof(s._jsonb->{key})='array' THEN s._jsonb->{key} WHEN s._jsonb->{key} IS NULL OR s._jsonb->{key}='null'::jsonb THEN '[]'::jsonb ELSE jsonb_build_array(s._jsonb->{key}) END");
                let mut output = format!("CASE WHEN element.ordinal IS NOT NULL THEN s._jsonb || jsonb_build_object({key},element.value) WHEN s._jsonb->{key}='[]'::jsonb THEN s._jsonb-{key} ELSE s._jsonb END");
                if let Some(index) = index {
                    if index.is_empty() || index.contains('.') || index.starts_with('$') {
                        bail!("includeArrayIndex currently requires a top-level field");
                    }
                    let index = sql.bind(encode_key(index), "text");
                    output = format!("({output}) || jsonb_build_object({index},CASE WHEN jsonb_typeof(s._jsonb->{key})='array' AND element.ordinal IS NOT NULL THEN jsonb_build_object('$i',(element.ordinal-1)::text) ELSE 'null'::jsonb END)");
                }
                let join = if preserve {
                    "LEFT JOIN LATERAL"
                } else {
                    "JOIN LATERAL"
                };
                format!("SELECT {output} AS _jsonb FROM ({source}) AS s {join} jsonb_array_elements({array}) WITH ORDINALITY AS element(value,ordinal) ON TRUE")
            }
            "$replaceRoot" | "$replaceWith" => {
                let root = if name == "$replaceRoot" {
                    let options = as_doc(value)?;
                    if options.len() != 1 {
                        bail!("$replaceRoot requires only newRoot");
                    }
                    options
                        .get("newRoot")
                        .ok_or_else(|| eyre!("$replaceRoot requires newRoot"))?
                } else {
                    value
                };
                let root = expression(root, sql, 0)?;
                // jsonb_each raises an error for non-documents. Native BSON
                // scalar tags must not masquerade as document roots either.
                format!("SELECT (SELECT COALESCE(jsonb_object_agg(key,value),'{{}}'::jsonb) FROM jsonb_each(CASE WHEN jsonb_typeof(root.v)='object' AND NOT root.v ?| ARRAY['$i','$f','$o','$d','$b'] THEN root.v ELSE 'false'::jsonb END)) AS _jsonb FROM ({source}) AS s CROSS JOIN LATERAL (SELECT {root} AS v OFFSET 0) AS root")
            }
            "$group" => group(&source, as_doc(value)?, sql)?,
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
        // Keep PostgreSQL from duplicating earlier relation joins and computed
        // expressions while flattening the nested stage queries. Each stage
        // consumes the preceding result once, as the pipeline describes.
        let name = format!("stage_{depth}_{stage_index}");
        stages.push(format!("{name} AS MATERIALIZED ({source})"));
        source = format!("SELECT _jsonb FROM {name}");
        if stages.iter().map(String::len).sum::<usize>() > 1024 * 1024 || sql.params.len() > 20_000
        {
            bail!("pipeline exceeds SQL complexity limit");
        }
    }
    if stages.is_empty() {
        Ok(source)
    } else {
        Ok(format!("WITH {} {source}", stages.join(",")))
    }
}
fn as_doc(value: &Bson) -> Result<&Document> {
    value
        .as_document()
        .ok_or_else(|| eyre!("stage requires a document"))
}
fn field(path: &str, sql: &mut Sql) -> Result<String> {
    field_from("_jsonb", path, sql, 0)
}

fn field_from(input: &str, path: &str, sql: &mut Sql, depth: usize) -> Result<String> {
    if path.is_empty() || path.split('.').any(str::is_empty) {
        bail!("invalid field path");
    }
    if depth > 8 {
        bail!("field path exceeds 8 levels");
    }
    let (key, rest) = path
        .split_once('.')
        .map_or((path, None), |(key, rest)| (key, Some(rest)));
    let key = sql.bind(encode_key(key), "text");
    let selected = format!("({input})->{key}");
    let Some(rest) = rest else {
        return Ok(selected);
    };
    let alias = format!("path_{depth}");
    let scalar = field_from(&format!("{alias}.v"), rest, sql, depth + 1)?;
    let item = field_from(&format!("{alias}_item.v"), rest, sql, depth + 1)?;
    Ok(format!("(SELECT CASE WHEN jsonb_typeof({alias}.v)='array' THEN (SELECT COALESCE(jsonb_agg(value ORDER BY ordinal) FILTER (WHERE value IS NOT NULL),'[]'::jsonb) FROM (SELECT {item} AS value, ordinal FROM jsonb_array_elements({alias}.v) WITH ORDINALITY AS {alias}_item(v,ordinal)) AS elements) ELSE {scalar} END FROM (SELECT {selected} AS v) AS {alias})"))
}
fn literal(value: &Bson, sql: &mut Sql) -> String {
    sql.bind(value.clone().into_psql_json(), "jsonb")
}

/// One expression compiler is shared by projections and group accumulators.
fn expression(value: &Bson, sql: &mut Sql, depth: usize) -> Result<String> {
    expression_with_vars(value, sql, depth, &std::collections::HashMap::new())
}

fn expression_with_vars(
    value: &Bson,
    sql: &mut Sql,
    depth: usize,
    vars: &std::collections::HashMap<String, String>,
) -> Result<String> {
    if depth > 64 {
        bail!("expression exceeds 64 levels");
    }
    let result = match value {
        Bson::String(value) if value.starts_with("$$") => {
            let (name, path) = value[2..]
                .split_once('.')
                .map_or((&value[2..], None), |(name, path)| (name, Some(path)));
            let input = match name {
                "REMOVE" => "NULL::jsonb".into(),
                "ROOT" | "CURRENT" => "_jsonb".into(),
                _ => vars
                    .get(name)
                    .cloned()
                    .ok_or_else(|| eyre!("undefined variable {name}"))?,
            };
            if let Some(path) = path {
                field_from(&input, path, sql, 0)?
            } else {
                input
            }
        }
        Bson::String(value) if value.starts_with('$') => field(&value[1..], sql)?,
        Bson::Array(values) => {
            let values = values
                .iter()
                .map(|v| expression_with_vars(v, sql, depth + 1, vars))
                .collect::<Result<Vec<_>>>()?;
            format!("jsonb_build_array({})", values.join(","))
        }
        Bson::Document(doc) if doc.keys().any(|k| k.starts_with('$')) => {
            if doc.len() != 1 {
                bail!("expression must have exactly one operator");
            }
            let (op, value) = doc.iter().next().expect("one expression field");
            match op.as_str() {
                "$concat" => {
                    let values = value
                        .as_array()
                        .ok_or_else(|| eyre!("$concat requires an array"))?;
                    let values = values
                        .iter()
                        .map(|v| expression_with_vars(v, sql, depth + 1, vars))
                        .collect::<Result<Vec<_>>>()?;
                    format!("(SELECT CASE WHEN COALESCE(bool_or(v='null'::jsonb),FALSE) THEN 'null'::jsonb ELSE to_jsonb(COALESCE(string_agg(CASE WHEN v='null'::jsonb THEN NULL WHEN jsonb_typeof(v)='string' THEN v#>>'{{}}' ELSE jsonb_array_length(jsonb_build_object('invalid',v))::text END,'' ORDER BY ordinal),'')) END FROM jsonb_array_elements(jsonb_build_array({})) WITH ORDINALITY AS concat_input(v,ordinal))",values.join(","))
                }
                "$mergeObjects" => {
                    let inputs = match value {
                        Bson::Array(inputs) => inputs.clone(),
                        value => vec![value.clone()],
                    };
                    let inputs = inputs
                        .iter()
                        .map(|value| expression_with_vars(value, sql, depth + 1, vars))
                        .collect::<Result<Vec<_>>>()?;
                    format!("(SELECT COALESCE(jsonb_object_agg(key,value ORDER BY ordinal),'{{}}'::jsonb) FROM jsonb_array_elements(jsonb_build_array({})) WITH ORDINALITY AS merged(document,ordinal) CROSS JOIN LATERAL jsonb_each(CASE WHEN merged.document='null'::jsonb THEN '{{}}'::jsonb WHEN jsonb_typeof(merged.document)='object' AND NOT merged.document ?| ARRAY['$i','$f','$o','$d','$b'] THEN merged.document ELSE 'false'::jsonb END) AS fields(key,value))",inputs.join(","))
                }

                "$cond" => {
                    let inputs = match value {
                        Bson::Array(values) if values.len() == 3 => values.clone(),
                        Bson::Document(doc) if doc.len() == 3 => vec![
                            doc.get("if")
                                .ok_or_else(|| eyre!("missing $cond if"))?
                                .clone(),
                            doc.get("then")
                                .ok_or_else(|| eyre!("missing $cond then"))?
                                .clone(),
                            doc.get("else")
                                .ok_or_else(|| eyre!("missing $cond else"))?
                                .clone(),
                        ],
                        _ => bail!("$cond requires if, then, else"),
                    };
                    let condition = expression_with_vars(&inputs[0], sql, depth + 1, vars)?;
                    let yes = expression_with_vars(&inputs[1], sql, depth + 1, vars)?;
                    let no = expression_with_vars(&inputs[2], sql, depth + 1, vars)?;
                    format!("CASE WHEN {} THEN {yes} ELSE {no} END", truthy(&condition))
                }
                "$eq" | "$gte" => {
                    let inputs = value
                        .as_array()
                        .filter(|inputs| inputs.len() == 2)
                        .ok_or_else(|| eyre!("{op} requires two operands"))?;
                    let left = expression_with_vars(&inputs[0], sql, depth + 1, vars)?;
                    let right = expression_with_vars(&inputs[1], sql, depth + 1, vars)?;
                    let operator = if op == "$eq" {
                        "IS NOT DISTINCT FROM"
                    } else {
                        ">="
                    };
                    format!(
                        "to_jsonb(COALESCE(({}) {operator} ({}),FALSE))",
                        crate::pg::identity_expr(&format!("({left})")),
                        crate::pg::identity_expr(&format!("({right})"))
                    )
                }
                "$let" => {
                    let doc = as_doc(value)?;
                    if doc
                        .keys()
                        .any(|key| !["vars", "in"].contains(&key.as_str()))
                    {
                        bail!("unsupported $let option");
                    }
                    let mut scoped = vars.clone();
                    let mut bindings = Vec::new();
                    for (name, value) in doc.get_document("vars")? {
                        let value = expression_with_vars(value, sql, depth + 1, vars)?;
                        let alias = crate::sql::identifier(name);
                        bindings.push(format!("{value} AS {alias}"));
                        scoped.insert(name.clone(), format!("let_{depth}.{alias}"));
                    }
                    let result = expression_with_vars(
                        doc.get("in").ok_or_else(|| eyre!("missing $let in"))?,
                        sql,
                        depth + 1,
                        &scoped,
                    )?;
                    if bindings.is_empty() {
                        result
                    } else {
                        format!(
                            "(SELECT {result} FROM (SELECT {}) AS let_{depth})",
                            bindings.join(",")
                        )
                    }
                }
                "$indexOfArray" | "$arrayElemAt" => {
                    let inputs = value
                        .as_array()
                        .filter(|inputs| inputs.len() == 2)
                        .ok_or_else(|| eyre!("{op} currently requires two operands"))?;
                    let input = expression_with_vars(&inputs[0], sql, depth + 1, vars)?;
                    let argument = expression_with_vars(&inputs[1], sql, depth + 1, vars)?;
                    let alias = format!("array_{depth}");
                    let result = if op == "$indexOfArray" {
                        format!("(SELECT to_jsonb(COALESCE(MIN(ordinal)-1,-1)) FROM jsonb_array_elements({alias}.input) WITH ORDINALITY AS elements(value,ordinal) WHERE {} IS NOT DISTINCT FROM {})", crate::pg::identity_expr("value"), crate::pg::identity_expr(&format!("{alias}.argument")))
                    } else {
                        format!(
                            "({alias}.input)->(({})::integer)",
                            parser::numeric(&format!("{alias}.argument"))
                        )
                    };
                    format!("(SELECT CASE WHEN {alias}.input IS NULL OR {alias}.input='null'::jsonb THEN 'null'::jsonb ELSE {result} END FROM (SELECT {input} AS input, {argument} AS argument) AS {alias})")
                }
                "$concatArrays" => {
                    let values = value
                        .as_array()
                        .ok_or_else(|| eyre!("$concatArrays requires an array"))?;
                    let values = values
                        .iter()
                        .map(|v| expression_with_vars(v, sql, depth + 1, vars))
                        .collect::<Result<Vec<_>>>()?;
                    format!("(SELECT CASE WHEN COALESCE(bool_or(v='null'::jsonb),FALSE) THEN 'null'::jsonb ELSE (SELECT COALESCE(jsonb_agg(element ORDER BY ordinal, element_ordinal),'[]'::jsonb) FROM jsonb_array_elements(jsonb_build_array({inputs})) WITH ORDINALITY AS arrays(v,ordinal) CROSS JOIN LATERAL jsonb_array_elements(v) WITH ORDINALITY AS elements(element,element_ordinal)) END FROM jsonb_array_elements(jsonb_build_array({inputs})) AS operands(v))", inputs=values.join(","))
                }
                "$reduce" => {
                    let doc = as_doc(value)?;
                    if doc
                        .keys()
                        .any(|k| !["input", "initialValue", "in"].contains(&k.as_str()))
                    {
                        bail!("unsupported $reduce option");
                    }
                    let input = expression_with_vars(
                        doc.get("input")
                            .ok_or_else(|| eyre!("missing $reduce input"))?,
                        sql,
                        depth + 1,
                        vars,
                    )?;
                    let initial = expression_with_vars(
                        doc.get("initialValue")
                            .ok_or_else(|| eyre!("missing $reduce initialValue"))?,
                        sql,
                        depth + 1,
                        vars,
                    )?;
                    let alias = format!("reduce_{depth}");
                    let mut scoped = vars.clone();
                    scoped.insert("value".into(), format!("{alias}.value"));
                    scoped.insert(
                        "this".into(),
                        format!("(reduce_input_{depth}.input)->({alias}.ordinal)"),
                    );
                    let body = expression_with_vars(
                        doc.get("in").ok_or_else(|| eyre!("missing $reduce in"))?,
                        sql,
                        depth + 1,
                        &scoped,
                    )?;
                    format!("(SELECT CASE WHEN reduce_input_{depth}.input IS NULL OR reduce_input_{depth}.input='null'::jsonb THEN 'null'::jsonb ELSE (WITH RECURSIVE {alias}(ordinal,value) AS (SELECT 0,COALESCE({initial},'null'::jsonb) UNION ALL SELECT {alias}.ordinal+1,COALESCE({body},'null'::jsonb) FROM {alias} WHERE {alias}.ordinal<jsonb_array_length(reduce_input_{depth}.input)) SELECT value FROM {alias} ORDER BY ordinal DESC LIMIT 1) END FROM (SELECT {input} AS input) AS reduce_input_{depth})")
                }
                "$map" | "$filter" => {
                    let doc = as_doc(value)?;
                    let input = expression_with_vars(
                        doc.get("input")
                            .ok_or_else(|| eyre!("missing {op} input"))?,
                        sql,
                        depth + 1,
                        vars,
                    )?;
                    let name = doc.get_str("as").unwrap_or("this");
                    if doc.contains_key("as") && doc.get_str("as").is_err() {
                        bail!("{op} as must be a string");
                    }
                    let allowed = if op == "$map" {
                        vec!["input", "as", "in"]
                    } else {
                        vec!["input", "as", "cond"]
                    };
                    if doc.keys().any(|key| !allowed.contains(&key.as_str())) {
                        bail!("unsupported {op} option");
                    }
                    let mut scoped = vars.clone();
                    let alias = format!("elements_{depth}");
                    scoped.insert(name.into(), format!("{alias}.value"));
                    let result = expression_with_vars(
                        doc.get(if op == "$map" { "in" } else { "cond" })
                            .ok_or_else(|| eyre!("missing {op} body"))?,
                        sql,
                        depth + 1,
                        &scoped,
                    )?;
                    let (output, predicate) = if op == "$map" {
                        (format!("COALESCE({result},'null'::jsonb)"), "TRUE".into())
                    } else {
                        (format!("{alias}.value"), truthy(&result))
                    };
                    format!("(SELECT CASE WHEN array_input.v IS NULL OR array_input.v='null'::jsonb THEN 'null'::jsonb ELSE (SELECT COALESCE(jsonb_agg({output} ORDER BY {alias}.ordinal),'[]'::jsonb) FROM jsonb_array_elements(array_input.v) WITH ORDINALITY AS {alias}(value,ordinal) WHERE {predicate}) END FROM (SELECT {input} AS v) AS array_input)")
                }
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
                        .map(|v| expression_with_vars(v, sql, depth + 1, vars))
                        .collect::<Result<Vec<_>>>()?;
                    let bindings = inputs
                        .iter()
                        .enumerate()
                        .map(|(i, input)| format!("{input} AS v{i}"))
                        .collect::<Vec<_>>()
                        .join(",");
                    let inputs = (0..inputs.len())
                        .map(|i| format!("arithmetic_{depth}.v{i}"))
                        .collect::<Vec<_>>();
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
                    let result = number_result(&terms.join(operator), &double, &long);
                    format!("(SELECT {result} FROM (SELECT {bindings}) AS arithmetic_{depth})")
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
                    let date = expression_with_vars(
                        doc.get("date").ok_or_else(|| eyre!("missing date"))?,
                        sql,
                        depth + 1,
                        vars,
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
                let value = expression_with_vars(value, sql, depth + 1, vars)?;
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
fn truthy(value: &str) -> String {
    format!("COALESCE(({value}) NOT IN ('null'::jsonb,'false'::jsonb,'0'::jsonb,'{{\"$i\":\"0\"}}'::jsonb,'{{\"$f\":0}}'::jsonb,'{{\"$f\":\"-0\"}}'::jsonb),FALSE)")
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
        if name != "$sum" && name != "$avg" && name != "$first" {
            bail!("unsupported accumulator {name}");
        }
        let value = expression(value, sql, 0)?;
        fields.push(sql.bind(encode_key(key), "text"));
        if name == "$first" {
            fields.push(format!(
                "COALESCE((array_agg({value} ORDER BY __group_ordinal))[1],'null'::jsonb)"
            ));
            continue;
        }
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
        "SELECT jsonb_build_object({}) AS _jsonb FROM (SELECT _jsonb, row_number() OVER () AS __group_ordinal FROM ({source}) AS group_input) AS s GROUP BY {id}",
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
        assert!(sql.text.find("LIMIT 5").unwrap() < sql.text.find("OFFSET 2").unwrap());
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
