use crate::utils::{collapse_fields, expand_doc};
use bson::{doc, Bson, Document};

use super::sql_statement::SqlStatement;

#[derive(Debug)]
pub struct InvalidProjectionError {
    pub message: String,
}

pub fn handle_oper(doc: &Document) -> Result<Option<String>, InvalidProjectionError> {
    let keys = doc.keys();
    let opers: Vec<&String> = keys.filter(|k| k.starts_with("$")).collect();
    if opers.len() < 1 {
        return Ok(None);
    }
    let oper = opers[0];
    match oper.as_str() {
        "$literal" => {
            let value = doc.get(oper).unwrap();
            match value {
                Bson::String(str) => Ok(Some(format!("'{}'", str))),
                _ => Ok(Some(value.to_string())),
            }
        }
        _ => Err(InvalidProjectionError {
            message: format!("Unsupported operator: {}", oper),
        }),
    }
}

pub fn handle_field(key: String, value: &Bson) -> Option<String> {
    match value {
        Bson::String(str) => match str.strip_prefix("$") {
            Some(str) => Some(format!("'{}', _jsonb->'{}'", key, str)),
            None => Some(format!("'{}', '{}'", key, str)),
        },
        Bson::Int32(_) => Some(format!("'{}', _jsonb->'{}'", key, key)),
        _ => Some(format!("'{}', {}", key, value.to_string())),
    }
}

pub fn arr_to_json_build_array(arr: &Vec<Bson>) -> Result<String, InvalidProjectionError> {
    let fields: Vec<String> = arr
        .iter()
        .map(|v| match v {
            Bson::String(str) => match str.strip_prefix("$") {
                Some(str) => format!("_jsonb->'{}'", str),
                None => format!("'{}'", str),
            },
            Bson::Int32(i) => format!("{}", i),
            _ => format!("{}", v.to_string()),
        })
        .collect();
    Ok(format!("json_build_array({})", fields.join(", ")))
}

pub fn doc_to_json_build_object(doc: &Document) -> Result<String, InvalidProjectionError> {
    let mut fields = vec![];
    for (key, value) in expand_doc(doc) {
        match value {
            Bson::Document(doc) => {
                // finds operations
                match handle_oper(&doc) {
                    Ok(value) => {
                        match value {
                            // is an operation, got the value back
                            Some(value) => fields.push(format!("'{}', {}", key, value)),

                            // no operation, let's parse as document
                            None => {
                                // if no operation, just insert the field
                                let doc = doc_to_json_build_object(&doc)?;
                                fields.push(format!("'{}', {}", key, doc))
                            }
                        }
                    }
                    Err(v) => return Err(v),
                }
            }
            Bson::Array(arr) => {
                fields.push(format!("'{}', {}", key, arr_to_json_build_array(&arr)?));
            }
            _ => match handle_field(key.clone(), &value) {
                Some(str) => fields.push(str),
                None => {
                    return Err(InvalidProjectionError {
                        message: format!("Unsupported value for key {}: {}", key, value),
                    })
                }
            },
        }
    }
    Ok(format!("json_build_object({})", fields.join(", ")))
}

pub fn process_project(doc: &Document) -> Result<SqlStatement, InvalidProjectionError> {
    let doc = &collapse_fields(doc);

    let mut sql = SqlStatement::new();

    let mut doc = doc.clone();
    if is_inclusion(&doc)? {
        match doc.get("_id") {
            Some(id) => {
                let keep_id = val_as_bool("_id".to_string(), id)
                    .unwrap()
                    .as_bool()
                    .unwrap();
                if !keep_id {
                    doc.remove("_id");
                }
                sql.add_field(&format!("{} AS _jsonb", &doc_to_json_build_object(&doc)?));
            }
            None => {
                let mut new_doc = doc! {
                    "_id": 1
                };
                for (key, value) in doc {
                    new_doc.insert(key.clone(), value.clone());
                }
                sql.add_field(&format!(
                    "{} AS _jsonb",
                    &doc_to_json_build_object(&new_doc)?
                ));
            }
        }
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
    // handling special operators
    let last = key.split(".").last().unwrap();

    // $literal
    if last == "$literal" {
        return Ok(Bson::Boolean(true));
    }

    // unsupported operators
    if last.starts_with("$") {
        return Err(InvalidProjectionError {
            message: format!(r#"Unrecognized expression "{}""#, last),
        });
    }

    match value {
        Bson::Int32(v) => Ok(Bson::Boolean(*v != 0)),
        Bson::Int64(v) => Ok(Bson::Boolean(*v != 0)),
        Bson::Double(v) => Ok(Bson::Boolean(*v != 0.0)),
        Bson::String(_) => Ok(Bson::Boolean(true)),
        Bson::Array(_) => Ok(Bson::Boolean(true)),
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
        let fields = doc_to_json_build_object(&flat).unwrap();
        assert_eq!(
            fields,
            "json_build_object('pick', _jsonb->'pick', 'num', 1, 'bool', true, 'str', 'value')"
        );
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
        let fields = process_project(&flat).unwrap();
        assert_eq!(
            fields.to_string(),
            "SELECT json_build_object('pick', _jsonb->'pick', 'num', 1, 'bool', true, 'str', 'value') AS _jsonb "
        );
    }

    #[test]
    fn test_process_inclusion_with_rename() {
        let doc = doc! {
            "field": "$from_field",
            "num": "$number",
            "non_field": "str value",
        };
        let flat = collapse_fields(&doc);
        let fields = doc_to_json_build_object(&flat).unwrap();
        assert_eq!(
            fields,
            "json_build_object('field', _jsonb->'from_field', 'num', _jsonb->'number', 'non_field', 'str value')"
        );
    }

    #[test]
    fn test_process_inclusion_with_nested_rename() {
        let doc = doc! {
            "field.one": "$from_field",
            "field.two": "$number",
            "field.three.$literal": "$number",
            "non_field": "str value",
        };
        let flat = collapse_fields(&doc);
        let fields = doc_to_json_build_object(&flat).unwrap();
        assert_eq!(
            fields,
            "json_build_object('field', json_build_object('one', _jsonb->'from_field', 'two', _jsonb->'number', 'three', '$number'), 'non_field', 'str value')"
        );
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

    #[test]
    fn test_rename() {
        let doc = doc! {
            "complete_name": "$name",
            "place": "$city",
            "attr.hair_color": "$hair",
            "attr.eyes_color": "$eyes",
        };
        let sql = process_project(&doc).unwrap();
        assert_eq!(sql.to_string(), "SELECT json_build_object('_id', _jsonb->'_id', 'complete_name', _jsonb->'name', 'place', _jsonb->'city', 'attr', json_build_object('hair_color', _jsonb->'hair', 'eyes_color', _jsonb->'eyes')) AS _jsonb ");
    }

    #[test]
    pub fn test_doc_to_json_object() {
        let doc = doc! {
            "include": 1,
            "complete_name": "$name",
            "place": "$city",
            "attr.hair_color": "$hair",
            "attr.eyes_color": "$eyes",
            "name.$literal": "Felipe",
            "age.$literal": 30,
        };
        let str = doc_to_json_build_object(&doc).unwrap();
        assert_eq!(str, "json_build_object('include', _jsonb->'include', 'complete_name', _jsonb->'name', 'place', _jsonb->'city', 'attr', json_build_object('hair_color', _jsonb->'hair', 'eyes_color', _jsonb->'eyes'), 'name', 'Felipe', 'age', 30)");
    }

    #[test]
    pub fn test_handle_oper() {
        assert_eq!("1", handle_oper(&doc! { "$literal": 1 }).unwrap().unwrap());
        assert_eq!(
            "'Felipe'",
            handle_oper(&doc! { "$literal": "Felipe" })
                .unwrap()
                .unwrap()
        );
        assert_eq!(None, handle_oper(&doc! { "name": 1 }).unwrap());
    }

    #[test]
    pub fn test_process_project_with_array() {
        let doc = doc! {
            "_id": 0,
            "myArray": ["$x", "$y"],
        };
        let sql = process_project(&doc).unwrap();
        assert_eq!(
            sql.to_string(),
            "SELECT json_build_object('myArray', json_build_array(_jsonb->'x', _jsonb->'y')) AS _jsonb "
        );
    }

    #[test]
    pub fn test_arr_to_json_build_array_with_fields() {
        let arr = vec![
            Bson::String("$x".to_string()),
            Bson::String("$y".to_string()),
        ];
        let str = arr_to_json_build_array(&arr).unwrap();
        assert_eq!(str, "json_build_array(_jsonb->'x', _jsonb->'y')");
    }

    #[test]
    pub fn test_arr_to_json_build_array_with_literal() {
        let arr = vec![
            Bson::Int32(1),
            Bson::Int32(2),
            Bson::String("Felipe".to_string()),
        ];
        let str = arr_to_json_build_array(&arr).unwrap();
        assert_eq!(str, "json_build_array(1, 2, 'Felipe')");
    }
}
