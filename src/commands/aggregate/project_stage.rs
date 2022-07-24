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

    if is_inclusion(doc)? {
        let fields = doc
            .iter()
            .map(|(key, _)| format!("'{}', _jsonb->'{}'", key, key))
            .collect::<Vec<String>>()
            .join(", ");

        sql.add_field(&format!(
            "json_build_object('_id', _jsonb->'_id', {}) AS _jsonb",
            fields
        ));
    } else {
        let fields = doc
            .iter()
            .map(|(key, _)| format!("'{}'", key))
            .collect::<Vec<String>>()
            .join(" - ");
        sql.add_field(&format!("_jsonb - {}", fields));
    }

    Ok(sql)
}

fn is_inclusion(doc: &Document) -> Result<bool, InvalidProjectionError> {
    let mut inclusion: Option<bool> = None;
    for (key, value) in doc.iter() {
        let value = match value {
            Bson::Int32(v) => Bson::Boolean(*v != 0),
            Bson::Int64(v) => Bson::Boolean(*v != 0),
            Bson::Double(v) => Bson::Boolean(*v != 0.0),
            _ => value.to_owned(),
        };

        match value {
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
    Ok(inclusion.unwrap())
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
}
