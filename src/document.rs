use bson::{bson, Bson, JavaScriptCodeWithScope};
use chrono::Utc;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::{
    convert::{TryFrom, TryInto},
    fmt::{self, Debug, Display, Formatter},
};

// Intermediate representation of a document as it is stored on the database.
//
// It uses some JSON representations for the BSON types, and it follows the same
// standards used by FerretDB:
//
// $f - for floating point numbers
// $o - for Object ID
// $d - for dates, stored as millis since epoch

trait PostgresSerializer {
    fn into_psql_json(self) -> Value;
}

impl PostgresSerializer for Bson {
    fn into_psql_json(self) -> Value {
        match self {
            Bson::Int32(i) => json!(i),
            Bson::Int64(i) => json!({ "$i": i.to_string() }),
            Bson::Double(f) if f.is_normal() => {
                let mut s = f.to_string();
                if f.fract() == 0.0 {
                    s.push_str(".0");
                }

                json!({ "$f": s })
            }
            Bson::Double(f) if f == 0.0 => {
                let s = if f.is_sign_negative() { "-0.0" } else { "0.0" };

                json!({ "$f": s })
            }
            Bson::DateTime(date) => {
                json!({ "$d": date.timestamp_millis().to_string() })
            }
            Bson::Array(arr) => Value::Array(arr.into_iter().map(Bson::into_psql_json).collect()),
            Bson::Document(arr) => Value::Object(
                arr.into_iter()
                    .map(|(k, v)| (k, v.into_psql_json()))
                    .collect(),
            ),
            Bson::JavaScriptCode(code) => json!({ "$j": code }),
            Bson::JavaScriptCodeWithScope(JavaScriptCodeWithScope { code, scope }) => json!({
                "$j": code,
                "s": Bson::Document(scope).into_psql_json(),
            }),
            Bson::RegularExpression(bson::Regex { pattern, options }) => {
                let mut chars: Vec<_> = options.chars().collect();
                chars.sort_unstable();

                let options: String = chars.into_iter().collect();

                json!({
                    "$r": pattern,
                    "o": options,
                })
            }
            Bson::ObjectId(v) => json!({"$o": v.to_hex()}),

            other => other.into_relaxed_extjson(),
        }
    }
}

#[cfg(test)]
mod tests {
    use bson::{doc, Bson};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[test]
    fn test_parse_float() {
        // let doc = doc! { "a": Bson::Double(1.0) };
        // let bson: Bson = doc.into();
        // let json: Value = bson.into_psql_json();
        let json = Bson::Double(1.0).into_psql_json().to_string();
        assert_eq!(r#"{"$f":"1.0"}"#, json);
    }

    #[test]
    fn test_parse_bson() {
        let local_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let doc = doc! {
          "float": Bson::Double(1.0),
          "int": Bson::Int32(1),
          "long": Bson::Int64(1),
          "string": "hello",
          "datetime": bson::DateTime::from_millis(local_time.try_into().unwrap()),
          "objectId": Bson::ObjectId(bson::oid::ObjectId::new()),
          "javascript": Bson::JavaScriptCode("function a() { return 'hey'; }".to_string()),
          "javascriptScope": Bson::JavaScriptCodeWithScope(bson::JavaScriptCodeWithScope{
            code: "function a() { return 'hey'; }".to_string(),
            scope: doc! { "a": 1, "b": 2 },
          }),
          "object": doc! {
            "a": 1,
            "b": 2,
          },
        };

        // let res = serde_json::to_string(&doc).unwrap();
        // let obj: Bson = serde_json::from_str(&res).unwrap();
        // println!("{}", res);
        // println!("{:#?}", obj);

        let bson: Bson = doc.into();

        let json: serde_json::Value = bson.clone().into();
        println!("{}", json); // { "x": 5, "_id": { "$oid": <hexstring> } }

        let relaxed_extjson = bson.clone().into_relaxed_extjson();
        println!("{}", relaxed_extjson); // { "x": 5, "_id": { "$oid": <hexstring> } }

        let into_psql_json = bson.into_psql_json();
        println!("{}", into_psql_json); // { "x": { "$numberInt": "5" }, "_id": { "$oid": <hexstring> } }
    }
}
