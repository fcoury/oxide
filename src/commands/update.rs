#![allow(dead_code)]
use crate::handler::{CommandExecutionError, Request};
use crate::{commands::Handler, pg::SqlParam};
use bson::{doc, Bson, Document};

pub struct Update {}

#[derive(Debug, Clone, PartialEq)]
pub enum UpdateOper {
    Update(Vec<UpdateDoc>),
    Replace(Document),
}

#[derive(Debug, Clone, PartialEq)]
pub enum UpdateDoc {
    Set(Document),
    Inc(Document),
}

#[derive(Debug, Clone, PartialEq)]
pub struct InvalidUpdateError {
    reason: String,
}

impl InvalidUpdateError {
    pub fn new(reason: String) -> Self {
        InvalidUpdateError { reason }
    }
}

impl UpdateDoc {
    fn validate(&self) -> Result<UpdateDoc, InvalidUpdateError> {
        match self {
            UpdateDoc::Set(doc) => match expand_fields(doc) {
                Ok(u) => Ok(UpdateDoc::Set(u)),
                Err(e) => {
                    return Err(InvalidUpdateError::new(format!(
                        "Cannot update '{}' and '{}' at the same time",
                        e.target, e.source
                    )));
                }
            },
            UpdateDoc::Inc(u) => Ok(UpdateDoc::Inc(u.clone())),
            // _ => {
            //     return Err(InvalidUpdateError::new(format!(
            //         "Unhandled update operation: {:?}",
            //         self
            //     )));
            // }
        }
    }
}

impl Handler for Update {
    fn new() -> Self {
        Update {}
    }

    fn handle(
        &self,
        request: &Request,
        docs: &Vec<Document>,
    ) -> Result<Document, CommandExecutionError> {
        let doc = &docs[0];
        let db = doc.get_str("$db").unwrap();
        let collection = doc.get_str("update").unwrap();
        let updates = doc.get_array("updates").unwrap();
        let sp = SqlParam::new(db, collection);

        let mut client = request.get_client();

        let mut n = 0;
        for update in updates {
            let doc = update.as_document().unwrap();
            let q = doc.get_document("q").unwrap();
            let update_doc = parse_update(doc.get_document("u").unwrap());
            let multi = doc.get_bool("multi").unwrap_or(true);

            if update_doc.is_err() {
                return Err(CommandExecutionError::new(format!("{:?}", update_doc)));
            }

            n += client
                .update(&sp, Some(q), update_doc.unwrap(), multi)
                .unwrap();
        }

        Ok(doc! {
            "n": Bson::Int64(n.try_into().unwrap()),
            "nModified": Bson::Int64(n as i64),
            "ok": Bson::Double(1.0),
        })
    }
}

#[derive(Debug, Clone)]
pub struct KeyConflictError {
    pub source: String,
    pub target: String,
}

fn path_to_doc(path: &str, value: &Bson) -> Document {
    let parts = path.split('.');

    let mut doc = doc! {};
    let mut first = true;
    for key in parts.rev() {
        if first {
            doc.insert(key, value.clone());
            first = false;
        } else {
            doc = doc! {
                key: doc
            };
        }
    }

    doc
}

pub fn expand_fields(doc: &Document) -> Result<Document, KeyConflictError> {
    let mut expanded = doc![];
    let mut keys: Vec<&str> = vec![];
    for (key, value) in doc.iter() {
        if key.contains(".") {
            let ikey = key.split(".").next().unwrap();
            if expanded.contains_key(ikey) {
                let target = keys
                    .iter()
                    .find(|k| {
                        k.to_string() == ikey.to_string() || k.starts_with(&format!("{}.", ikey))
                    })
                    .unwrap();
                return Err(KeyConflictError {
                    source: key.to_string(),
                    target: target.to_string(),
                });
            }
            expanded.insert(ikey, path_to_doc(key, value).get(ikey).unwrap());
        } else {
            expanded.insert(key, value);
        }
        keys.push(&key);
    }
    Ok(expanded)
}

fn get_path(doc: &Document, path: String) -> Option<&Bson> {
    let parts: Vec<&str> = path.split(".").collect();
    let mut current = doc;
    for part in parts {
        match current.get_document(part) {
            Ok(doc) => current = doc,
            Err(_) => return current.get(part),
        }
    }
    None
}

fn parse_update(doc: &Document) -> Result<UpdateOper, InvalidUpdateError> {
    let mut res: Vec<UpdateDoc> = vec![];
    if !doc.keys().any(|k| k.starts_with("$")) {
        return Ok(UpdateOper::Replace(doc.clone()));
    }
    for (key, value) in doc.iter() {
        match key.as_str() {
            "$set" => {
                let expanded_doc = match expand_fields(value.as_document().unwrap()) {
                    Ok(doc) => doc,
                    Err(e) => {
                        return Err(InvalidUpdateError::new(format!(
                            "Cannot update '{}' and '{}' at the same time",
                            e.target, e.source
                        )));
                    }
                };
                match UpdateDoc::Set(expanded_doc).validate() {
                    Ok(update_doc) => res.push(update_doc),
                    Err(e) => {
                        return Err(InvalidUpdateError::new(format!("{:?}", e)));
                    }
                }
            }
            "$inc" => {
                let expanded_doc = match expand_fields(value.as_document().unwrap()) {
                    Ok(doc) => doc,
                    Err(e) => {
                        return Err(InvalidUpdateError::new(format!(
                            "Cannot update '{}' and '{}' at the same time",
                            e.target, e.source
                        )));
                    }
                };
                match UpdateDoc::Inc(expanded_doc).validate() {
                    Ok(update_doc) => res.push(update_doc),
                    Err(e) => {
                        return Err(InvalidUpdateError::new(format!("{:?}", e)));
                    }
                }
            }
            _ => {
                if key.starts_with("$") || res.len() > 0 {
                    return Err(InvalidUpdateError::new(format!(
                        "Unknown modifier: {}",
                        key
                    )));
                }
            }
        }
    }
    Ok(UpdateOper::Update(res))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_to_doc() {
        let doc = path_to_doc("a.b.c", &Bson::Int32(1));
        assert_eq!(
            doc,
            doc! {
                "a": {
                    "b": {
                        "c": 1
                    }
                }
            }
        );
    }

    #[test]
    fn test_get_path() {
        assert_eq!(
            get_path(&doc! {"x": {"y": {"z": 1}}}, "x.y.z".to_string()).unwrap(),
            &Bson::Int32(1)
        );
        assert_eq!(get_path(&doc! {}, "a.b.c".to_string()), None);
    }

    #[test]
    fn test_expand_fields() {
        let expanded =
            expand_fields(&doc! { "z": 1, "a.b": 1, "b.c.d": 2, "x.y.z": "Felipe" }).unwrap();
        assert_eq!(
            expanded,
            doc! { "z": 1, "a": { "b": 1 }, "b": { "c": { "d": 2 } }, "x": { "y" : { "z": "Felipe" } } }
        );
    }

    #[test]
    fn test_expand_fields_with_conflict() {
        let expanded = expand_fields(&doc! { "a.b": 1, "a.b.c": 2 });
        assert!(expanded.is_err());
        let err = expanded.unwrap_err();
        assert_eq!(err.source, "a.b.c");
        assert_eq!(err.target, "a.b");
    }

    #[test]
    fn test_parse_update() {
        let set_doc = doc! { "$set": { "a": 1 } };
        let repl_doc = doc! { "b": 2, "c": 8, "d": 9 };
        let unknown_doc = doc! { "$xyz": { "a": 1 } };
        let mixed_doc = doc! { "$set": { "x": 1 }, "b": 2 };

        assert_eq!(
            parse_update(&set_doc).unwrap(),
            UpdateOper::Update(vec![UpdateDoc::Set(doc! { "a": 1 })])
        );
        assert_eq!(
            parse_update(&repl_doc).unwrap(),
            UpdateOper::Replace(repl_doc)
        );
        assert_eq!(
            parse_update(&unknown_doc).unwrap_err(),
            InvalidUpdateError::new("Unknown modifier: $xyz".to_string())
        );
        assert_eq!(
            parse_update(&mixed_doc).unwrap_err(),
            InvalidUpdateError::new("Unknown modifier: b".to_string())
        );
    }
}
