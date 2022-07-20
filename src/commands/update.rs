#![allow(dead_code)]
use crate::handler::{CommandExecutionError, Request};
use crate::utils::expand_fields;
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
    Inc(Document),
    Set(Document),
    Unset(Document),
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
            UpdateDoc::Unset(doc) => Ok(UpdateDoc::Unset(doc.clone())),
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
        client.create_table_if_not_exists(db, collection).unwrap();

        let mut n = 0;
        for update in updates {
            let doc = update.as_document().unwrap();
            let q = doc.get_document("q").unwrap();
            let update_doc = parse_update(doc.get_document("u").unwrap());
            let upsert = doc.get_bool("upsert").unwrap_or(false);
            let multi = doc.get_bool("multi").unwrap_or(false);

            if update_doc.is_err() {
                return Err(CommandExecutionError::new(format!("{:?}", update_doc)));
            }

            n += client
                .update(&sp, Some(q), update_doc.unwrap(), upsert, multi)
                .unwrap();
        }

        Ok(doc! {
            "n": Bson::Int64(n.try_into().unwrap()),
            "nModified": Bson::Int64(n as i64),
            "ok": Bson::Double(1.0),
        })
    }
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
            "$unset" => {
                let expanded_doc = match expand_fields(value.as_document().unwrap()) {
                    Ok(doc) => doc,
                    Err(e) => {
                        return Err(InvalidUpdateError::new(format!(
                            "Cannot update '{}' and '{}' at the same time",
                            e.target, e.source
                        )));
                    }
                };
                match UpdateDoc::Unset(expanded_doc).validate() {
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
