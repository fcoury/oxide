use bson::{Bson, Document};

use crate::utils::collapse_fields;

use super::sql_statement::SqlStatement;

#[derive(Debug)]
pub struct InvalidProjectionError {
    pub message: String,
}

pub fn process_inclusion(doc: &Document) -> Result<Vec<String>, InvalidProjectionError> {
    let mut res = vec![];

    let mut doc = doc.clone();

    if doc.contains_key("_id") {
        let include = val_as_bool("_id".to_string(), &doc.get("_id").unwrap());
        match include {
            Ok(Bson::Boolean(true)) => (),
            Ok(Bson::Boolean(false)) => {
                doc.remove("_id");
            },
            Ok(v) => unimplemented!("Unexpected result of val_as_boolean evaluating _id inclusion on process_inclusion: {:?}", v),
            Err(v) => return Err(v),
        }
    } else {
        res.push("'_id', _jsonb->'_id'".to_string());
    }

    for (key, value) in doc.iter() {
        let parts = key.split(".");
        let count = parts.clone().count();
        let last = parts.clone().last().unwrap();
        if last == "$literal" {
            let field = parts
                .take(count - 1)
                .map(|f| format!("'{}'", f))
                .collect::<Vec<_>>()
                .join("->");
            match value.as_str() {
                Some(v) => res.push(format!("{}, '{}'", field, v)),
                None => res.push(format!("{}, {}", field, value.to_string())),
            }
        } else {
            res.push(format!("'{}', _jsonb->'{}'", key, key));
        }
    }

    Ok(res)
}

pub fn process_project(doc: &Document) -> Result<SqlStatement, InvalidProjectionError> {
    let doc = &collapse_fields(doc);

    let mut sql = SqlStatement::new();

    if is_inclusion(doc)? {
        let fields = process_inclusion(doc)?;
        sql.add_field(&format!(
            "json_build_object({}) AS _jsonb",
            fields.join(", ")
        ));
    } else {
        let has_id = doc.contains_key("_id");
        let include_id = has_id
            && match val_as_bool("_id".to_string(), doc.get("_id").unwrap()) {
                Ok(v) => v.as_bool().unwrap(),
                Err(v) => return Err(v),
            };

        let fields = doc
            .iter()
            .filter(|(key, _)| !include_id || key.as_str() != "_id")
            .map(|(key, _)| format!("'{}'", key))
            .collect::<Vec<String>>()
            .join(" - ");

        sql.add_field(&format!("_jsonb - {} AS _jsonb", fields));
    }

    Ok(sql)
}

fn val_as_bool(key: String, value: &Bson) -> Result<Bson, InvalidProjectionError> {
    let last = key.split(".").last().unwrap();

    if last == "$literal" {
        return Ok(Bson::Boolean(true));
    }

    if last.starts_with("$") {
        return Err(InvalidProjectionError {
            message: format!(r#"Unrecognized expression "{}""#, last),
        });
    }

    match value {
        Bson::Int32(v) => Ok(Bson::Boolean(*v != 0)),
        Bson::Int64(v) => Ok(Bson::Boolean(*v != 0)),
        Bson::Double(v) => Ok(Bson::Boolean(*v != 0.0)),
        Bson::Document(v) => {
            let keys: Vec<&String> = v.keys().collect();
            if keys.len() > 1 {
                return Err(InvalidProjectionError {
                    message: format!(
                        "an expression especification must contain exactly one field, the name of the expression. Found {} fields in {:?}.",
                        keys.len(), v,
                    )
                });
            }
            if keys[0].ends_with("$literal") {
                Ok(Bson::Boolean(true))
            } else {
                Err(InvalidProjectionError {
                    message: format!(r#"Unrecognized expression "{}""#, keys[0]),
                })
            }
        }
        _ => Ok(value.to_owned()),
    }
}

fn is_inclusion(doc: &Document) -> Result<bool, InvalidProjectionError> {
    let mut inclusion: Option<bool> = None;
    for (key, value) in doc.iter() {
        if key == "_id" {
            continue;
        }

        match val_as_bool(key.clone(), value) {
            Ok(Bson::Boolean(v)) => {
                if let Some(inclusion) = inclusion {
                    if v {
                        if !inclusion {
                            return Err(InvalidProjectionError {
                                message: format!(
                                    "Cannot do exclusion of field {} in inclusion project",
                                    key
                                ),
                            });
                        }
                    } else {
                        if inclusion {
                            return Err(InvalidProjectionError {
                                message: format!(
                                    "Cannot do inclusion of field {} in exclusion project",
                                    key
                                ),
                            });
                        }
                    }
                }
                inclusion = Some(v);
            }
            Err(v) => return Err(v),
            t => unimplemented!("{:?}", t),
        }
    }
    Ok(inclusion.unwrap_or(false))
}

#[cfg(test)]
mod tests {
    use bson::doc;

    use super::*;

    #[test]
    fn test_is_inclusion_true() {
        let doc = doc! {
            "field1": 1,
            "field2": true,
            "field3": 1.0,
        };
        assert_eq!(is_inclusion(&doc).unwrap(), true);
    }

    #[test]
    fn test_is_inclusion_true_with_id_exclusion() {
        let doc = doc! {
            "field1": 1,
            "field2": true,
            "field3": 1.0,
            "_id": 0,
        };
        assert_eq!(is_inclusion(&doc).unwrap(), true);
    }

    #[test]
    fn test_is_inclusion_false() {
        let doc = doc! {
            "field1": 0,
            "field2": false,
            "field3": 0.0,
        };
        assert_eq!(is_inclusion(&doc).unwrap(), false);
    }

    #[test]
    fn test_is_inclusion_exclusion_error() {
        let doc = doc! {
            "field1": 0,
            "field2": true,
            "field3": 0.0,
        };
        assert!(is_inclusion(&doc).is_err());
        assert_eq!(
            is_inclusion(&doc).unwrap_err().message,
            "Cannot do exclusion of field field2 in inclusion project"
        );
    }

    #[test]
    fn test_is_exclusion_inclusion_error() {
        let doc = doc! {
            "field1": 1,
            "field2": false,
            "field3": 1.0,
        };
        assert!(is_inclusion(&doc).is_err());
        assert_eq!(
            is_inclusion(&doc).unwrap_err().message,
            "Cannot do inclusion of field field2 in exclusion project"
        );
    }

    #[test]
    fn test_id_exclusion_on_inclusion() {
        let doc = doc! {
            "_id": 0,
            "field1": 1,
        };
        let sql = process_project(&doc).unwrap();
        assert_eq!(
            sql.to_string(),
            "SELECT json_build_object('field1', _jsonb->'field1') AS _jsonb "
        );
    }

    #[test]
    fn test_id_inclusion_on_exclusion() {
        let doc = doc! {
            "field1": 0,
            "_id": 1,
        };
        let sql = process_project(&doc).unwrap();
        assert_eq!(sql.to_string(), "SELECT _jsonb - 'field1' AS _jsonb ");
    }

    #[test]
    fn test_id_exclusion_on_exclusion() {
        let doc = doc! {
            "_id": 0,
        };
        let sql = process_project(&doc).unwrap();
        assert_eq!(sql.to_string(), "SELECT _jsonb - '_id' AS _jsonb ");
    }

    #[test]
    fn test_process_inclusion() {
        let doc = doc! {
            "pick": 1,
            "num": { "$literal": 1 },
            "bool": { "$literal": true },
            "str": { "$literal": "value" },
        };
        let flat = collapse_fields(&doc);
        let fields = process_inclusion(&flat).unwrap();
        assert_eq!(fields[0], "'_id', _jsonb->'_id'");
        assert_eq!(fields[1], "'pick', _jsonb->'pick'");
        assert_eq!(fields[2], "'num', 1");
        assert_eq!(fields[3], "'bool', true");
        assert_eq!(fields[4], "'str', 'value'");
    }

    #[test]
    fn test_process_inclusion_excluding_id() {
        let doc = doc! {
            "_id": 0,
            "pick": 1,
            "num": { "$literal": 1 },
            "bool": { "$literal": true },
            "str": { "$literal": "value" },
        };
        let flat = collapse_fields(&doc);
        let fields = process_inclusion(&flat).unwrap();
        assert_eq!(fields[0], "'pick', _jsonb->'pick'");
        assert_eq!(fields[1], "'num', 1");
        assert_eq!(fields[2], "'bool', true");
        assert_eq!(fields[3], "'str', 'value'");
    }

    #[test]
    fn test_literal() {
        let doc = doc! {
            "pick": 1,
            "num": { "$literal": 1 },
            "bool": { "$literal": true },
            "str": { "$literal": "value" },
        };
        let sql = process_project(&doc).unwrap();
        assert_eq!(sql.to_string(), "SELECT json_build_object('_id', _jsonb->'_id', 'pick', _jsonb->'pick', 'num', 1, 'bool', true, 'str', 'value') AS _jsonb ");
    }
}
