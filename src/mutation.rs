//! Apply a complete document mutation while PgDb holds its row lock.
use crate::parser::{UpdateDoc, UpdateOper};
use bson::{Bson, Document};
use eyre::{bail, eyre, Result};

pub fn apply(before: &Document, update: &UpdateOper) -> Result<Document> {
    let mut after = match update {
        UpdateOper::Replace(doc) => {
            let mut replacement = doc.clone();
            if let Some(id) = before.get("_id") {
                if replacement.get("_id").is_some_and(|new| new != id) {
                    bail!("_id is immutable");
                }
                replacement.insert("_id", id.clone());
            }
            replacement
        }
        UpdateOper::Update(operations) => {
            let mut after = before.clone();
            for operation in operations {
                let fields = match operation {
                    UpdateDoc::Set(d)
                    | UpdateDoc::Unset(d)
                    | UpdateDoc::Inc(d)
                    | UpdateDoc::AddToSet(d) => d,
                };
                for (path, value) in fields {
                    let parts = path.split('.').collect::<Vec<_>>();
                    change(&mut after, &parts, value, operation)?;
                }
            }
            after
        }
    };
    if !after.contains_key("_id") {
        after.insert("_id", bson::oid::ObjectId::new());
    }
    Ok(after)
}

fn change(doc: &mut Document, path: &[&str], value: &Bson, operation: &UpdateDoc) -> Result<()> {
    if path.len() > 64 {
        bail!("update nesting exceeds 64 levels");
    }
    let key = path[0];
    if path.len() > 1 {
        if !doc.contains_key(key) {
            if matches!(operation, UpdateDoc::Unset(_)) {
                return Ok(());
            }
            doc.insert(key, Document::new());
        }
        let nested = doc
            .get_document_mut(key)
            .map_err(|_| eyre!("cannot traverse non-document field '{key}'"))?;
        return change(nested, &path[1..], value, operation);
    }
    match operation {
        UpdateDoc::Set(_) => {
            doc.insert(key, value.clone());
        }
        UpdateDoc::Unset(_) => {
            doc.remove(key);
        }
        UpdateDoc::Inc(_) => {
            let new = add(doc.get(key), value)?;
            doc.insert(key, new);
        }
        UpdateDoc::AddToSet(_) => {
            if !doc.contains_key(key) {
                doc.insert(key, Bson::Array(Vec::new()));
            }
            if value
                .as_document()
                .is_some_and(|d| d.contains_key("$each") && d.len() != 1)
            {
                bail!("unsupported $addToSet modifier");
            }
            let values = match value.as_document().and_then(|d| d.get("$each")) {
                Some(each) => each
                    .as_array()
                    .ok_or_else(|| eyre!("$each requires an array"))?
                    .clone(),
                None => vec![value.clone()],
            };
            let array = doc.get_array_mut(key).map_err(|_| {
                eyre!("Cannot apply $addToSet to a non-array field. Field named '{key}'")
            })?;
            for value in values {
                if !array.contains(&value) {
                    array.push(value);
                }
            }
        }
    }
    Ok(())
}

fn add(current: Option<&Bson>, increment: &Bson) -> Result<Bson> {
    fn number(b: &Bson) -> Result<f64> {
        match b {
            Bson::Int32(i) => Ok(*i as f64),
            Bson::Int64(i) => Ok(*i as f64),
            Bson::Double(f) if f.is_finite() => Ok(*f),
            _ => bail!("$inc requires numeric fields and operands"),
        }
    }
    number(increment)?;
    let Some(current) = current else {
        return Ok(increment.clone());
    };
    number(current)?;
    if matches!(current, Bson::Double(_)) || matches!(increment, Bson::Double(_)) {
        let result = number(current)? + number(increment)?;
        if !result.is_finite() {
            bail!("$inc overflow");
        }
        return Ok(Bson::Double(result));
    }
    let integer = |v: &Bson| match v {
        Bson::Int32(i) => *i as i64,
        Bson::Int64(i) => *i,
        _ => unreachable!(),
    };
    let sum = integer(current)
        .checked_add(integer(increment))
        .ok_or_else(|| eyre!("$inc overflow"))?;
    Ok(
        if matches!((current, increment), (Bson::Int32(_), Bson::Int32(_))) {
            i32::try_from(sum)
                .map(Bson::Int32)
                .unwrap_or(Bson::Int64(sum))
        } else {
            Bson::Int64(sum)
        },
    )
}

/// Equality fields form the seed of an operator upsert; range/logical operators
/// are not copied into the inserted document.
pub fn upsert_seed(query: Option<&Document>) -> Result<Document> {
    let mut seed = Document::new();
    if let Some(query) = query {
        for (field, value) in query {
            if field.starts_with('$') {
                continue;
            }
            let value = match value.as_document() {
                Some(d) if d.len() == 1 && d.contains_key("$eq") => {
                    d.get("$eq").expect("checked equality field")
                }
                Some(d) if d.keys().any(|k| k.starts_with('$')) => continue,
                _ => value,
            };
            change(
                &mut seed,
                &field.split('.').collect::<Vec<_>>(),
                value,
                &UpdateDoc::Set(Document::new()),
            )?;
        }
    }
    Ok(seed)
}
