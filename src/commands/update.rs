#![allow(dead_code)]
use crate::handler::{CommandExecutionError, Request};
use crate::{commands::Handler, pg::SqlParam};
use bson::{doc, Bson, Document};

pub struct Update {}

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
            let u = doc.get_document("u").unwrap();

            let set = match expand_fields(u.get_document("$set").unwrap()) {
                Ok(u) => u,
                Err(e) => {
                    return Err(CommandExecutionError {
                        message: format!(
                            "Cannot update '{}' and '{}' at the same time",
                            e.target, e.source
                        ),
                    })
                }
            };

            let mut u = u.clone();
            u.insert("$set", set);

            let multi = doc.get_bool("multi").unwrap_or(true);

            n += client.update(&sp, Some(q), &u, multi).unwrap();
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
                    .find(|k| k.starts_with(&format!("{}.", ikey)))
                    .unwrap();
                return Err(KeyConflictError {
                    source: key.to_string(),
                    target: target.to_string(),
                });
            }
            expanded.insert(ikey, path_to_doc(key, value).get(ikey).unwrap());
            keys.push(&key);
        } else {
            expanded.insert(key, value);
        }
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
}
