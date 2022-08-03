use bson::{Bson, Document};

use crate::utils::collapse_fields;

use super::sql_statement::SqlStatement;

#[derive(Debug)]
pub struct InvalidProjectionError {
    pub message: String,
}

pub fn process_project(doc: &Document) -> Result<SqlStatement, InvalidProjectionError> {
    let doc = &collapse_fields(doc);

    let mut sql = SqlStatement::new();

    let has_id = doc.contains_key("_id");
    let include_id = has_id && val_as_bool(doc.get("_id").unwrap()).as_bool().unwrap();

    if is_inclusion(doc)? {
        let mut fields = doc
            .iter()
            .map(|(k, _)| k.to_string())
            .filter(|k| k != "_id")
            .collect::<Vec<_>>();

        if include_id {
            fields.insert(0, "_id".to_string());
        }

        let fields = fields
            .iter()
            .map(|key| format!("'{}', _jsonb->'{}'", key, key))
            .collect::<Vec<String>>()
            .join(", ");

        sql.add_field(&format!("json_build_object({}) AS _jsonb", fields));
    } else {
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

fn val_as_bool(value: &Bson) -> Bson {
    match value {
        Bson::Int32(v) => Bson::Boolean(*v != 0),
        Bson::Int64(v) => Bson::Boolean(*v != 0),
        Bson::Double(v) => Bson::Boolean(*v != 0.0),
        _ => value.to_owned(),
    }
}

fn is_inclusion(doc: &Document) -> Result<bool, InvalidProjectionError> {
    let mut inclusion: Option<bool> = None;
    for (key, value) in doc.iter() {
        if key == "_id" {
            continue;
        }

        match val_as_bool(value) {
            Bson::Boolean(v) => {
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
}
