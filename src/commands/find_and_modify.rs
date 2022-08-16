use crate::handler::{CommandExecutionError, Request};
use crate::parser::parse_update;
use crate::pg::UpdateResult;
use crate::{commands::Handler, pg::SqlParam};
use bson::{doc, Bson, Document};

pub struct FindAndModify {}

impl Handler for FindAndModify {
    fn new() -> Self {
        FindAndModify {}
    }

    fn handle(
        &self,
        request: &Request,
        docs: &Vec<Document>,
    ) -> Result<Document, CommandExecutionError> {
        let doc = &docs[0];
        let db = doc.get_str("$db").unwrap();
        let collection = doc.get_str("findAndModify").unwrap();
        let sp = SqlParam::new(db, collection);
        let query = doc.get_document("query").unwrap();
        let raw_update = doc.get_document("update").unwrap();
        let update_doc = parse_update(raw_update);
        let upsert = doc.get_bool("upsert").unwrap_or(false);

        let mut client = request.get_client();
        client.create_table_if_not_exists(db, collection).unwrap();

        let res = client
            .update(&sp, Some(query), update_doc.unwrap(), false, false, true)
            .unwrap();

        match res {
            UpdateResult::Count(total) => {
                if total == 0 {
                    if upsert {
                        let mut obj = query.clone();
                        obj.extend(extract_operator_values(&raw_update));

                        let res = client.insert_doc(sp, &obj).unwrap();
                        return Ok(doc! {
                            "value": null,
                            "lastErrorObject": {
                                "updatedExisting": false,
                                "upserted": res.get_object_id("_id").unwrap().to_string(),
                                "n": 1,
                            },
                            "ok": 1.0,
                        });
                    } else {
                        return Ok(doc! {
                            "value": null,
                            "ok": Bson::Double(1.0),
                        });
                    }
                } else {
                    unreachable!(
                        "Unexpected numeric result for a findAndUpdate command: {:#?}",
                        doc
                    );
                }
            }
            UpdateResult::Document(value) => Ok(doc! {
                "n": Bson::Int64(1),
                "value": value,
                "ok": Bson::Double(1.0),
            }),
        }
    }
}

fn extract_operator_values(doc: &Document) -> Document {
    let mut res = Document::new();
    for (key, value) in doc {
        if key.starts_with("$") {
            if let Some(value) = value.as_document() {
                res.extend(value.clone());
            }
        }
    }
    res
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_operator_values() {
        assert_eq!(
            extract_operator_values(&doc! { "$inc": { "score": 1 }, "$set": { "name": "abc" } }),
            doc! { "score": 1, "name": "abc" }
        );
    }
}
