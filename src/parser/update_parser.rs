use bson::Document;
use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum UpdateOper {
    Update(Vec<UpdateDoc>),
    Replace(Document),
}
#[derive(Debug, Clone, PartialEq)]
pub enum UpdateDoc {
    Inc(Document),
    Set(Document),
    Unset(Document),
    AddToSet(Document),
}
#[derive(Debug, Clone, PartialEq)]
pub struct InvalidUpdateError {
    pub reason: String,
}
impl InvalidUpdateError {
    pub fn new(reason: String) -> Self {
        Self { reason }
    }
}
impl fmt::Display for InvalidUpdateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.reason)
    }
}
impl std::error::Error for InvalidUpdateError {}

pub fn parse_update(doc: &Document) -> Result<UpdateOper, InvalidUpdateError> {
    if !doc.keys().any(|k| k.starts_with('$')) {
        return Ok(UpdateOper::Replace(doc.clone()));
    }
    let mut operations = Vec::new();
    let mut paths: Vec<String> = Vec::new();
    for (operator, value) in doc {
        let fields = value
            .as_document()
            .ok_or_else(|| InvalidUpdateError::new(format!("{operator} requires a document")))?
            .clone();
        for path in fields.keys() {
            if path
                .split('.')
                .any(|p| p.is_empty() || p.starts_with('$') || p.contains('\0'))
            {
                return Err(InvalidUpdateError::new("unsupported update path".into()));
            }
            if path == "_id" || path.starts_with("_id.") {
                return Err(InvalidUpdateError::new("_id is immutable".into()));
            }
            if let Some(prior) = paths.iter().find(|p| {
                **p == *path
                    || p.starts_with(&format!("{path}."))
                    || path.starts_with(&format!("{p}."))
            }) {
                return Err(InvalidUpdateError::new(format!(
                    "Cannot update '{prior}' and '{path}' at the same time"
                )));
            }
            paths.push(path.clone());
        }
        operations.push(match operator.as_str() {
            "$set" => UpdateDoc::Set(fields),
            "$unset" => UpdateDoc::Unset(fields),
            "$inc" => UpdateDoc::Inc(fields),
            "$addToSet" => UpdateDoc::AddToSet(fields),
            _ => {
                return Err(InvalidUpdateError::new(format!(
                    "Unknown update operator: {operator}"
                )))
            }
        });
    }
    Ok(UpdateOper::Update(operations))
}
