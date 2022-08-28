#![allow(dead_code)]
use crate::deserializer::PostgresJsonDeserializer;
use bson::{doc, Bson, Document};
use postgres::Row;
use regex::Regex;
use serde_json::{json, Map, Value};
use std::ffi::CString;

#[derive(Debug, Clone)]
pub struct KeyConflictError {
    pub source: String,
    pub target: String,
}

impl std::error::Error for KeyConflictError {}

impl std::fmt::Display for KeyConflictError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "Conflicting keys '{}' and '{}'",
            self.source, self.target
        )
    }
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
    let re1 = Regex::new(r"([0-9a-f]{4}\s+)").unwrap();
    let re2 = Regex::new(r"(\s{3}(.*)$)").unwrap();
    let mut res = "".to_string();
    for line in op_msg_hexstr.split("\n") {
        let line = re1.replace_all(line, "").to_string();
        let line = re2.replace_all(&line, "").to_string();
        res = format!("{res} {line}");
    }
    hexstring_to_bytes(&res)
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

pub fn expand_doc(in_doc: &Document) -> Document {
    let mut doc = doc! {};
    for (key, value) in in_doc.iter() {
        if key.contains(".") {
            let mut parts = key.splitn(2, ".");
            let k = parts.next().unwrap();
            let rest = parts.next().unwrap();

            let mut tmp_doc = match doc.get(k) {
                Some(d) => match d.as_document() {
                    Some(d) => d.clone(),
                    None => {
                        let mut final_doc = doc! {};
                        final_doc.insert(k, d);
                        final_doc.clone()
                    }
                },
                None => doc! {},
            };

            tmp_doc.insert(rest, value.clone());

            doc.insert(k, expand_doc(&tmp_doc));
        } else {
            doc.insert(key, value);
        }
    }
    doc
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

pub fn pg_rows_to_bson(rows: Vec<Row>) -> Vec<Bson> {
    let mut res: Vec<Bson> = vec![];
    for row in rows.iter() {
        let json_value: serde_json::Value = row.get(0);
        let bson_value = json_value.from_psql_json();
        res.push(bson_value);
    }
    res
}

pub fn field_to_jsonb(key: &str) -> String {
    format!("_jsonb->'{}'", key)
}

pub fn convert_if_numeric(field: &str) -> String {
    format!(
        "CASE WHEN ({} ? '$f') THEN ({}->>'$f')::numeric ELSE ({})::numeric END",
        field, field, field
    )
}

#[cfg(test)]
mod test {
    use indoc::indoc;

    use super::*;

    #[test]
    fn test_expand_fields() {
        let doc = doc! {
            "a.b.c.d": 1,
            "a.b.d": 2,
            "a.b.c.f": 3,
            "b": 4,
        };

        let expanded = expand_doc(&doc);
        assert_eq!(
            expanded,
            doc! {
                "a": {
                    "b": {
                        "c": {
                            "d": 1,
                            "f": 3
                        },
                        "d": 2
                    }
                },
                "b": 4,
            }
        );
    }

    #[test]
    fn test_hexdump_to_bytes() {
        let insert = indoc! {"
            0000   c9 00 00 00 36 00 00 00 00 00 00 00 dd 07 00 00   ....6...........
            0010   00 00 00 00 00 33 00 00 00 02 69 6e 73 65 72 74   .....3....insert
            0020   00 0a 00 00 00 69 6e 76 65 6e 74 6f 72 79 00 08   .....inventory..
            0030   6f 72 64 65 72 65 64 00 01 02 24 64 62 00 05 00   ordered...$db...
            0040   00 00 74 65 73 74 00 00 01 80 00 00 00 64 6f 63   ..test.......doc
            0050   75 6d 65 6e 74 73 00 72 00 00 00 07 5f 69 64 00   uments.r...._id.
            0060   63 0b ac 82 29 0b 4a 69 98 af 9c ac 02 69 74 65   c...).Ji.....ite
            0070   6d 00 07 00 00 00 63 61 6e 76 61 73 00 10 71 74   m.....canvas..qt
            0080   79 00 64 00 00 00 04 74 61 67 73 00 13 00 00 00   y.d....tags.....
            0090   02 30 00 07 00 00 00 63 6f 74 74 6f 6e 00 00 03   .0.....cotton...
            00a0   73 69 7a 65 00 23 00 00 00 10 68 00 1c 00 00 00   size.#....h.....
            00b0   01 77 00 00 00 00 00 00 c0 41 40 02 75 6f 6d 00   .w.......A@.uom.
            00c0   03 00 00 00 63 6d 00 00 00                        ....cm...
        "};
        let bytes = hexdump_to_bytes(insert);
        assert_eq!(bytes.len(), 201);
    }
}
