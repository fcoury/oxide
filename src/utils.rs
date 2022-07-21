#![allow(dead_code)]
use bson::{doc, Bson, Document};
use regex::Regex;
use serde_json::{json, Map, Value};
use std::ffi::CString;

#[derive(Debug, Clone)]
pub struct KeyConflictError {
    pub source: String,
    pub target: String,
}

pub fn to_cstring(buffer: Vec<u8>) -> String {
    let str = unsafe { CString::from_vec_unchecked(buffer) }
        .to_string_lossy()
        .to_string();
    return str;
}

pub fn hexstring_to_bytes(hexstr: &str) -> Vec<u8> {
    let re = Regex::new(r"((\d|[a-f]){2})").unwrap();
    let mut bytes: Vec<u8> = vec![];
    for cap in re.captures_iter(hexstr) {
        bytes.push(u8::from_str_radix(cap.get(1).unwrap().as_str(), 16).unwrap());
    }
    return bytes;
}

pub fn hexdump_to_bytes(op_msg_hexstr: &str) -> Vec<u8> {
    let re = Regex::new(r"\d{4}\s{3}(((\d|[a-f]){2}\s)+)\s{2}.*").unwrap();
    let mut bytes: Vec<u8> = vec![];
    for cap in re.captures_iter(op_msg_hexstr) {
        bytes.extend(hexstring_to_bytes(cap.get(1).unwrap().as_str()));
    }
    return bytes;
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

fn path_to_obj(path: &str, value: &serde_json::Value) -> Map<String, serde_json::Value> {
    let parts = path.split('.');

    let mut doc = Map::new();
    let mut first = true;
    for key in parts.rev() {
        if first {
            doc.insert(key.to_owned(), value.clone());
            first = false;
        } else {
            doc = json!({ key: doc }).as_object().unwrap().clone();
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

pub fn collapse_fields(doc: &Document) -> Document {
    let mut collapsed = doc![];
    for (key, value) in doc.iter() {
        if value.as_document().is_none() {
            collapsed.insert(key, value);
            continue;
        }

        let res = collapse_fields(value.as_document().unwrap());
        for (k, v) in res {
            collapsed.insert(format!("{}.{}", key, k), v);
        }
    }
    collapsed
}

pub fn flatten_object(obj: &Map<String, Value>) -> Map<String, Value> {
    let mut collapsed = Map::new();
    for (key, value) in obj.iter() {
        if !value.is_object() {
            collapsed.insert(key.clone(), value.clone());
            continue;
        }

        let res = flatten_object(value.as_object().unwrap());
        for (k, v) in res {
            collapsed.insert(format!("{}.{}", key, k), v);
        }
    }
    collapsed.clone()
}

pub fn expand_object(obj: &Map<String, Value>) -> Result<Map<String, Value>, KeyConflictError> {
    let mut expanded = Map::new();
    let mut keys: Vec<&str> = vec![];
    for (key, value) in obj.iter() {
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
            expanded.insert(
                ikey.to_owned(),
                path_to_obj(key, value).get(ikey).unwrap().to_owned(),
            );
        } else {
            expanded.insert(key.to_owned(), value.to_owned());
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
    fn test_collapse_fields() {
        let nested = doc! { "a": 1, "b": { "c": 2, "d": 3, "e": { "f": 1 } } };
        let collapsed = collapse_fields(&nested);
        assert_eq!(collapsed, doc! { "a": 1, "b.c": 2, "b.d": 3, "b.e.f": 1 });
    }
}
