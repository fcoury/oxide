use crate::{
    commands::{check_options, Handler},
    handler::{CommandExecutionError, Request},
    pg::{integer, optional_bool, SqlParam, MAX_QUERY_ROWS},
};
use bson::{doc, Bson, Document};
pub struct Find;
impl Handler for Find {
    fn new() -> Self {
        Self
    }
    fn handle(
        &self,
        request: &Request,
        docs: &[Document],
    ) -> Result<Document, CommandExecutionError> {
        let doc = &docs[0];
        check_options(
            doc,
            &[
                "find",
                "filter",
                "sort",
                "projection",
                "skip",
                "limit",
                "batchSize",
                "singleBatch",
                "hint",
            ],
        )?;
        let sp = SqlParam::from(doc, "find")?;
        let filter = optional_document(doc, "filter")?
            .cloned()
            .unwrap_or_default();
        let sort = optional_document(doc, "sort")?;
        if let Some(hint) = doc.get("hint") {
            request.get_client()?.validate_hint(&sp, hint)?;
        }
        let projection = optional_document(doc, "projection")?;
        let skip = optional_integer(doc, "skip", 0)?;
        let limit = optional_integer(doc, "limit", 0)?;
        if skip < 0 || limit == i64::MIN {
            return Err(CommandExecutionError::new("invalid skip or limit".into()));
        }
        let batch = optional_integer(doc, "batchSize", 101)?;
        if batch < 0 {
            return Err(CommandExecutionError::new(
                "batchSize must be nonnegative".into(),
            ));
        }
        let single = optional_bool(doc, "singleBatch", false)? || limit < 0;
        crate::parser::compile(&filter)?;
        let mut client = request.get_client()?;
        let mut results = if sp.exists(&mut client)? {
            client.find(&sp, &filter, sort, skip, limit.abs())?
        } else {
            Vec::new()
        };
        if let Some(projection) = projection {
            validate_projection(projection)?;
            for result in &mut results {
                *result = project(result, projection)?;
            }
        }
        Ok(request
            .cursors
            .lock()
            .map_err(|_| CommandExecutionError::new("cursor lock unavailable".into()))?
            .first(
                format!("{}.{}", sp.db, sp.collection),
                results,
                (batch as usize).min(MAX_QUERY_ROWS),
                single,
            )?)
    }
}
pub fn optional_document<'a>(
    doc: &'a Document,
    key: &str,
) -> Result<Option<&'a Document>, CommandExecutionError> {
    match doc.get(key) {
        None => Ok(None),
        Some(Bson::Document(d)) => Ok(Some(d)),
        _ => Err(CommandExecutionError::new(format!(
            "{key} must be a document"
        ))),
    }
}
pub fn optional_integer(
    doc: &Document,
    key: &str,
    default: i64,
) -> Result<i64, CommandExecutionError> {
    Ok(match doc.get(key) {
        None => default,
        Some(value) => integer(value)?,
    })
}
fn flag(value: &Bson) -> Result<bool, CommandExecutionError> {
    match value {
        Bson::Boolean(b) => Ok(*b),
        Bson::Int32(i) => Ok(*i != 0),
        Bson::Int64(i) => Ok(*i != 0),
        Bson::Double(f) => Ok(*f != 0.0),
        _ => Err(CommandExecutionError::new(
            "projection supports only inclusion/exclusion flags".into(),
        )),
    }
}
fn validate_projection(projection: &Document) -> Result<(), CommandExecutionError> {
    let mut mode = None;
    for (key, value) in projection {
        let value = flag(value)?;
        if key.contains('$') || key.split('.').any(str::is_empty) {
            return Err(CommandExecutionError::new(
                "unsupported projection path".into(),
            ));
        }
        if key != "_id" {
            if mode.is_some_and(|mode| mode != value) {
                return Err(CommandExecutionError::new(
                    "cannot mix inclusion and exclusion".into(),
                ));
            }
            mode = Some(value);
        }
    }
    Ok(())
}
pub fn project(
    source: &Document,
    projection: &Document,
) -> Result<Document, CommandExecutionError> {
    if projection.is_empty() {
        return Ok(source.clone());
    }
    let inclusion = projection
        .iter()
        .filter(|(key, _)| key.as_str() != "_id")
        .any(|(_, value)| flag(value).unwrap_or(false))
        || (projection.len() == 1
            && projection
                .get("_id")
                .is_some_and(|v| flag(v).unwrap_or(false)));
    let mut result = if inclusion {
        Document::new()
    } else {
        source.clone()
    };
    for (key, value) in projection {
        let include = flag(value)?;
        if key == "_id" {
            continue;
        }
        if inclusion && include {
            if let Some(value) = get_path(source, key)? {
                set_path(
                    &mut result,
                    &key.split('.').collect::<Vec<_>>(),
                    Some(value.clone()),
                )?;
            }
        } else if !inclusion && !include {
            set_path(&mut result, &key.split('.').collect::<Vec<_>>(), None)?;
        }
    }
    if projection.get("_id").map(flag).transpose()?.unwrap_or(true) {
        if let Some(id) = source.get("_id") {
            result.insert("_id", id);
        }
    } else {
        result.remove("_id");
    }
    if inclusion {
        order_like_source(&mut result, source);
    }
    Ok(result)
}
fn order_like_source(result: &mut Document, source: &Document) {
    let mut ordered = Document::new();
    for (key, value) in source {
        if let Some(mut selected) = result.remove(key) {
            if let (Bson::Document(selected), Bson::Document(original)) = (&mut selected, value) {
                order_like_source(selected, original);
            }
            ordered.insert(key, selected);
        }
    }
    ordered.extend(std::mem::take(result));
    *result = ordered;
}
fn get_path<'a>(
    source: &'a Document,
    path: &str,
) -> Result<Option<&'a Bson>, CommandExecutionError> {
    let mut parts = path.split('.');
    let mut value = source.get(parts.next().unwrap_or(""));
    for part in parts {
        value = match value {
            Some(Bson::Document(d)) => d.get(part),
            Some(Bson::Array(_)) => {
                return Err(CommandExecutionError::new(
                    "projection through arrays is unsupported".into(),
                ))
            }
            _ => None,
        };
    }
    Ok(value)
}
fn set_path(
    doc: &mut Document,
    path: &[&str],
    value: Option<Bson>,
) -> Result<(), CommandExecutionError> {
    if path.len() == 1 {
        if let Some(value) = value {
            doc.insert(path[0], value);
        } else {
            doc.remove(path[0]);
        }
        return Ok(());
    }
    if !doc.contains_key(path[0]) {
        if value.is_none() {
            return Ok(());
        }
        doc.insert(path[0], doc! {});
    }
    match doc.get_mut(path[0]) {
        Some(Bson::Document(nested)) => set_path(nested, &path[1..], value),
        Some(Bson::Array(_)) => Err(CommandExecutionError::new(
            "projection through arrays is unsupported".into(),
        )),
        _ => Ok(()),
    }
}
