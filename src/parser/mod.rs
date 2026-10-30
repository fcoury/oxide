//! Compile the supported BSON query subset directly, without a lossy JSON grammar.
use crate::{
    serializer::{encode_key, PostgresSerializer},
    sql::Sql,
};
use bson::{Bson, Document};
use eyre::{bail, Result};
mod update_parser;
pub use update_parser::{parse_update, InvalidUpdateError, UpdateDoc, UpdateOper};

pub fn compile(doc: &Document) -> Result<Sql> {
    if bson::to_vec(doc)?.len() > 64 * 1024 {
        bail!("filter exceeds 64 KiB");
    }
    let mut sql = Sql::default();
    sql.text = clauses(doc, &mut sql, 0)?;
    Ok(sql)
}

pub fn append_filter(doc: &Document, sql: &mut Sql) -> Result<String> {
    clauses(doc, sql, 0)
}

fn clauses(doc: &Document, sql: &mut Sql, depth: usize) -> Result<String> {
    if depth > 64 {
        bail!("query nesting exceeds 64 levels");
    }
    let mut parts = Vec::new();
    for (field, value) in doc {
        if field.starts_with('$') {
            if field == "$where" {
                // The supported field-reference form is evaluated natively. General
                // server-side JavaScript is deliberately not executed by this engine.
                let code = match value {
                    Bson::String(s) | Bson::JavaScriptCode(s) => s,
                    _ => bail!("$where requires JavaScript source"),
                };
                let code = code.trim();
                let field = code
                    .strip_prefix("this.")
                    .filter(|path| {
                        path.split('.').all(|part| {
                            !part.is_empty()
                                && part.chars().enumerate().all(|(i, c)| {
                                    c == '_'
                                        || c == '$'
                                        || c.is_ascii_alphabetic()
                                        || (i > 0 && c.is_ascii_digit())
                                })
                        })
                    })
                    .ok_or_else(|| {
                        eyre::eyre!("only field-reference $where expressions are supported")
                    })?;
                let path = path(field, sql)?;
                parts.push(exists(&path,"v NOT IN ('null'::jsonb,'false'::jsonb,'0'::jsonb,'{\"$i\":\"0\"}'::jsonb,'{\"$f\":0}'::jsonb,'{\"$f\":\"-0\"}'::jsonb,'\"\"'::jsonb,'{\"$f\":\"NaN\"}'::jsonb) AND (v->'$b'->>4) IS DISTINCT FROM '6'", false));
                continue;
            }
            let join = match field.as_str() {
                "$and" => " AND ",
                "$or" | "$nor" => " OR ",
                _ => bail!("unsupported query operator {field}"),
            };
            let values = value
                .as_array()
                .ok_or_else(|| eyre::eyre!("{field} requires an array"))?;
            if values.is_empty() {
                bail!("{field} requires a nonempty array");
            }
            let mut terms = Vec::new();
            for value in values {
                terms.push(clauses(
                    value
                        .as_document()
                        .ok_or_else(|| eyre::eyre!("{field} requires documents"))?,
                    sql,
                    depth + 1,
                )?);
            }
            let term = format!("({})", terms.join(join));
            parts.push(if field == "$nor" {
                format!("NOT {term}")
            } else {
                term
            });
        } else {
            parts.push(condition(field, value, sql, depth + 1)?);
        }
    }
    Ok(if parts.is_empty() {
        "TRUE".into()
    } else {
        format!("({})", parts.join(" AND "))
    })
}

fn path(field: &str, sql: &mut Sql) -> Result<String> {
    if field.contains('\0') || field.is_empty() {
        bail!("invalid field path");
    }
    let mut path = "$".to_string();
    for part in field.split('.') {
        if part.is_empty() {
            bail!("empty field path component");
        }
        path.push('.');
        path.push_str(&serde_json::to_string(&encode_key(part))?);
    }
    Ok(sql.bind(path, "jsonpath"))
}

fn condition(field: &str, value: &Bson, sql: &mut Sql, depth: usize) -> Result<String> {
    if depth > 64 {
        bail!("query nesting exceeds 64 levels");
    }
    if let Bson::RegularExpression(regex) = value {
        return regex_condition(field, &regex.pattern, &regex.options, sql);
    }
    if let Bson::Document(doc) = value {
        if doc.keys().any(|k| k.starts_with('$')) {
            let mut parts = Vec::new();
            for (op, operand) in doc {
                let part = match op.as_str() {
                    "$eq" | "$lt" | "$lte" | "$gt" | "$gte" => compare(field, operand, op, sql)?,
                    "$ne" => {
                        if matches!(operand, Bson::RegularExpression(_)) {
                            bail!("$ne cannot take a regex");
                        }
                        format!("NOT ({})", compare(field, operand, "$eq", sql)?)
                    }
                    "$type" => type_condition(field, operand, sql)?,
                    "$exists" => {
                        let exists = match operand {
                            Bson::Boolean(b) => *b,
                            Bson::Int32(i) => *i != 0,
                            Bson::Int64(i) => *i != 0,
                            Bson::Double(f) => *f != 0.0,
                            Bson::Null | Bson::Undefined => false,
                            _ => true,
                        };
                        let path = path(field, sql)?;
                        format!(
                            "{}jsonb_path_exists(_jsonb, {path})",
                            if exists { "" } else { "NOT " }
                        )
                    }
                    "$in" | "$nin" => {
                        let values = operand
                            .as_array()
                            .ok_or_else(|| eyre::eyre!("{op} requires an array"))?;
                        let mut terms = Vec::new();
                        for value in values {
                            terms.push(match value {
                                Bson::RegularExpression(regex) => {
                                    regex_condition(field, &regex.pattern, &regex.options, sql)?
                                }
                                Bson::Document(d) if d.keys().any(|k| k.starts_with('$')) => {
                                    bail!("operator documents are not supported inside $in/$nin")
                                }
                                _ => compare(field, value, "$eq", sql)?,
                            });
                        }
                        let matched = if terms.is_empty() {
                            "FALSE".into()
                        } else {
                            terms.join(" OR ")
                        };
                        format!("{}({matched})", if op == "$nin" { "NOT " } else { "" })
                    }
                    "$not" => format!("NOT ({})", condition(field, operand, sql, depth + 1)?),
                    "$regex" => {
                        let pattern = operand
                            .as_str()
                            .ok_or_else(|| eyre::eyre!("$regex has to be a string"))?;
                        let options = doc.get_str("$options").unwrap_or("");
                        if doc.contains_key("$options") && doc.get_str("$options").is_err() {
                            bail!("$options must be a string");
                        }
                        regex_condition(field, pattern, options, sql)?
                    }
                    "$options" if doc.contains_key("$regex") => continue,
                    _ => bail!("unsupported query operator {op}"),
                };
                parts.push(part);
            }
            return Ok(format!("({})", parts.join(" AND ")));
        }
        // Retain the project's nested predicate shorthand. A $eq document is
        // available when exact embedded-document equality is required.
        if !doc.is_empty() {
            let mut parts = Vec::new();
            for (key, value) in doc {
                parts.push(condition(&format!("{field}.{key}"), value, sql, depth + 1)?);
            }
            return Ok(format!("({})", parts.join(" AND ")));
        }
    }
    compare(field, value, "$eq", sql)
}

fn regex_condition(field: &str, pattern: &str, options: &str, sql: &mut Sql) -> Result<String> {
    if options.chars().any(|c| !"imsx".contains(c)) {
        bail!("unsupported regex option");
    }
    regex::RegexBuilder::new(pattern)
        .case_insensitive(options.contains('i'))
        .multi_line(options.contains('m'))
        .dot_matches_new_line(options.contains('s'))
        .ignore_whitespace(options.contains('x'))
        .build()?;
    let path = path(field, sql)?;
    // PostgreSQL: p = dot excludes newline; n = anchors and dot are
    // newline-sensitive; w = multiline anchors with dot matching newline.
    let mut flags = match (options.contains('m'), options.contains('s')) {
        (false, false) => "p",
        (true, false) => "n",
        (true, true) => "w",
        (false, true) => "s",
    }
    .to_string();
    if options.contains('i') {
        flags.push('i');
    }
    if options.contains('x') {
        flags.push('x');
    }
    let pattern = sql.bind(
        if flags.is_empty() {
            pattern.to_string()
        } else {
            format!("(?{flags}){pattern}")
        },
        "text",
    );
    Ok(exists(
        &path,
        &format!("jsonb_typeof(v) = 'string' AND (v #>> '{{}}') ~ {pattern}"),
        true,
    ))
}

pub fn numeric(field: &str) -> String {
    format!("CASE WHEN jsonb_typeof({field}) = 'number' THEN ({field})::numeric WHEN jsonb_typeof({field}->'$i') = 'string' THEN ({field}->>'$i')::numeric WHEN {field}->>'$f' = '-0' THEN 0 WHEN jsonb_typeof({field}->'$f') = 'number' THEN ({field}->>'$f')::numeric END")
}

fn compare(field: &str, value: &Bson, op: &str, sql: &mut Sql) -> Result<String> {
    if op != "$eq" && matches!(value, Bson::RegularExpression(_)) {
        bail!("{op} cannot take a regex");
    }
    let operator = match op {
        "$eq" => "=",
        "$lt" => "<",
        "$lte" => "<=",
        "$gt" => ">",
        "$gte" => ">=",
        _ => unreachable!(),
    };
    let path = path(field, sql)?;
    let predicate = match value {
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) => {
            let value = match value {
                Bson::Int32(i) => i.to_string(),
                Bson::Int64(i) => i.to_string(),
                Bson::Double(f) if f.is_finite() => f.to_string(),
                _ => bail!("nonfinite numeric comparisons are unsupported"),
            };
            let parameter = sql.bind(value, "numeric");
            format!("({}) {operator} {parameter}", numeric("v"))
        }
        Bson::DateTime(date) => {
            let value = sql.bind(date.timestamp_millis(), "bigint");
            format!("(v->>'$d')::bigint {operator} {value}")
        }
        _ => {
            let value = sql.bind(value.clone().into_psql_json(), "jsonb");
            format!("jsonb_typeof(v) = jsonb_typeof({value}) AND v {operator} {value}")
        }
    };
    let matched = exists(&path, &predicate, true);
    Ok(if value == &Bson::Null && op == "$eq" {
        format!("({matched} OR NOT jsonb_path_exists(_jsonb, {path}))")
    } else {
        matched
    })
}

fn type_condition(field: &str, operand: &Bson, sql: &mut Sql) -> Result<String> {
    let operands = match operand {
        Bson::Array(values) => values.as_slice(),
        value => std::slice::from_ref(value),
    };
    if operands.is_empty() {
        bail!("$type requires at least one type");
    }
    let mut predicates = Vec::new();
    for operand in operands {
        let name = match operand {
            Bson::String(name) => name.as_str(),
            value => match crate::pg::integer(value)? {
                -1 => "minKey",
                1 => "double",
                2 => "string",
                3 => "object",
                4 => "array",
                5 => "binData",
                6 => "undefined",
                7 => "objectId",
                8 => "bool",
                9 => "date",
                10 => "null",
                11 => "regex",
                13 => "javascript",
                14 => "symbol",
                15 => "javascriptWithScope",
                16 => "int",
                17 => "timestamp",
                18 => "long",
                19 => "decimal",
                127 => "maxKey",
                _ => bail!("invalid BSON type"),
            },
        };
        let predicate = match name {
            "double" => "v ? '$f'".into(),
            "string" => "(jsonb_typeof(v)='string' OR (v->'$b'->>4)='2')".into(),
            "object" => {
                "(jsonb_typeof(v)='object' AND NOT v ?| ARRAY['$i','$f','$o','$d','$b'])".into()
            }
            "array" => "jsonb_typeof(v)='array'".into(),
            "objectId" => "v ? '$o'".into(),
            "bool" => "jsonb_typeof(v)='boolean'".into(),
            "date" => "v ? '$d'".into(),
            "null" => "v='null'::jsonb".into(),
            "int" => "jsonb_typeof(v)='number'".into(),
            "long" => "v ? '$i'".into(),
            "number" => {
                "(jsonb_typeof(v)='number' OR v ?| ARRAY['$i','$f'] OR (v->'$b'->>4)='19')".into()
            }
            name => {
                let code = match name {
                    "binData" => 5,
                    "undefined" => 6,
                    "regex" => 11,
                    "javascript" => 13,
                    "symbol" => 14,
                    "javascriptWithScope" => 15,
                    "timestamp" => 17,
                    "decimal" => 19,
                    "minKey" => 255,
                    "maxKey" => 127,
                    _ => bail!("invalid BSON type {name}"),
                };
                format!("(v->'$b'->>4)='{code}'")
            }
        };
        let path = path(field, sql)?;
        predicates.push(exists(&path, &predicate, name != "array"));
    }
    Ok(format!("({})", predicates.join(" OR ")))
}

fn exists(path: &str, predicate: &str, unwrap_array: bool) -> String {
    let values = if unwrap_array {
        "SELECT v FROM candidates UNION ALL SELECT e FROM candidates, LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(v)='array' THEN v ELSE '[]'::jsonb END) AS a(e)"
    } else {
        "SELECT v FROM candidates"
    };
    format!("EXISTS (WITH candidates AS (SELECT v FROM jsonb_path_query(_jsonb,{path}) AS q(v)) SELECT 1 FROM ({values}) AS values_(v) WHERE {predicate})")
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;
    #[test]
    fn untrusted_fields_and_values_are_parameters() {
        let sql = compile(&doc! {"name' IS NOT NULL OR TRUE --": "O'Reilly"}).unwrap();
        assert!(!sql.text.contains("O'Reilly"));
        assert!(!sql.text.contains("OR TRUE"));
        assert!(sql.params.iter().any(|p| p.contains("O'Reilly")));
    }
    #[test]
    fn rejects_unknown_and_malformed_operators() {
        for doc in [
            doc! {"x":{"$bad":1}},
            doc! {"$or":[]},
            doc! {"x":{"$in":1}},
            doc! {"x":{"$not":{"$bad":1}}},
        ] {
            assert!(compile(&doc).is_err());
        }
    }
    #[test]
    fn nor_negates_disjunction() {
        let sql = compile(&doc! {
            "$nor": [doc! {"name": "Peter"}, doc! {"x": 3}]
        })
        .unwrap();
        assert!(sql.text.contains("NOT ("));
        assert!(sql.text.contains(" OR "));
    }
}
