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
                    | UpdateDoc::AddToSet(d)
                    | UpdateDoc::Push(d) => d,
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
    let id = after.remove("_id").expect("identity assigned");
    Ok(std::iter::once(("_id".to_owned(), id))
        .chain(after)
        .collect())
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
        return change_value(
            doc.get_mut(key).expect("field initialized"),
            &path[1..],
            value,
            operation,
        );
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
        UpdateDoc::AddToSet(_) | UpdateDoc::Push(_) => {
            let name = if matches!(operation, UpdateDoc::Push(_)) {
                "$push"
            } else {
                "$addToSet"
            };
            if !doc.contains_key(key) {
                doc.insert(key, Bson::Array(Vec::new()));
            }
            if value
                .as_document()
                .is_some_and(|d| d.contains_key("$each") && d.len() != 1)
            {
                bail!("unsupported {name} modifier");
            }
            let values = match value.as_document().and_then(|d| d.get("$each")) {
                Some(each) => each
                    .as_array()
                    .ok_or_else(|| eyre!("$each requires an array"))?
                    .clone(),
                None => vec![value.clone()],
            };
            let array = doc.get_array_mut(key).map_err(|_| {
                eyre!("Cannot apply {name} to a non-array field. Field named '{key}'")
            })?;
            for value in values {
                if matches!(operation, UpdateDoc::Push(_))
                    || !array.iter().any(|existing| equal(existing, &value))
                {
                    array.push(value);
                }
            }
        }
    }
    Ok(())
}

fn change_value(
    current: &mut Bson,
    path: &[&str],
    value: &Bson,
    operation: &UpdateDoc,
) -> Result<()> {
    match current {
        Bson::Document(doc) => change(doc, path, value, operation),
        Bson::Array(array) => {
            let index = path[0]
                .parse::<usize>()
                .ok()
                .filter(|_| path[0] == "0" || !path[0].starts_with('0'))
                .ok_or_else(|| eyre!("array update requires a numeric index"))?;
            let absent = index >= array.len();
            if absent {
                if matches!(operation, UpdateDoc::Unset(_)) {
                    return Ok(());
                }
                // A BSON array cannot contain this many null elements within 16 MiB.
                if index > 1_500_000 {
                    bail!("array update exceeds BSON size limit");
                }
                array.resize(index + 1, Bson::Null);
                if path.len() > 1 {
                    array[index] = Bson::Document(Document::new());
                }
            }
            if path.len() > 1 {
                return change_value(&mut array[index], &path[1..], value, operation);
            }
            if matches!(operation, UpdateDoc::Unset(_)) {
                array[index] = Bson::Null;
                return Ok(());
            }
            let mut field = if absent {
                Document::new()
            } else {
                bson::doc! {"value":array[index].clone()}
            };
            change(&mut field, &["value"], value, operation)?;
            array[index] = field
                .remove("value")
                .expect("array mutation returns a value");
            Ok(())
        }
        _ => bail!("cannot traverse non-document field"),
    }
}

/// MongoDB set membership compares numeric values across BSON numeric widths,
/// but embedded document field order remains significant.
fn equal(left: &Bson, right: &Bson) -> bool {
    match (left, right) {
        (Bson::Document(a), Bson::Document(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b)
                    .all(|((ak, av), (bk, bv))| ak == bk && equal(av, bv))
        }
        (Bson::Array(a), Bson::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| equal(a, b))
        }
        (Bson::Int32(a), Bson::Int64(b)) | (Bson::Int64(b), Bson::Int32(a)) => *a as i64 == *b,
        (Bson::Double(a), Bson::Double(b)) => a == b || (a.is_nan() && b.is_nan()),
        (Bson::Double(f), Bson::Int32(i)) | (Bson::Int32(i), Bson::Double(f)) => *f == *i as f64,
        (Bson::Double(f), Bson::Int64(i)) | (Bson::Int64(i), Bson::Double(f)) => {
            f.is_finite()
                && f.fract() == 0.0
                && *f >= i64::MIN as f64
                && *f < 9223372036854775808.0
                && *f as i64 == *i
        }
        _ => left == right,
    }
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
