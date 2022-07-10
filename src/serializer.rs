use bson::{Bson, JavaScriptCodeWithScope};
use serde_json::{json, Value};

// Intermediate representation of a document as it is stored on the database.
//
// It uses some JSON representations for the BSON types, and it follows the same
// standards used by FerretDB:
//
// $f - for floating point numbers
// $o - for Object ID
// $d - for dates, stored as millis since epoch

pub trait PostgresSerializer {
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
                json!({ "$d": date.timestamp_millis() })
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
                "s": serde_json::to_string(&scope).unwrap(),
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
    fn test_parse_string() {
        let json = Bson::String("hello".into()).into_psql_json().to_string();
        assert_eq!(r#""hello""#, json);
    }

    #[test]
    fn test_parse_int32() {
        let json = Bson::Int32(1).into_psql_json().to_string();
        assert_eq!(r#"1"#, json);
    }

    #[test]
    fn test_parse_int64() {
        let json = Bson::Int64(1).into_psql_json().to_string();
        assert_eq!(r#"{"$i":"1"}"#, json);
    }

    #[test]
    fn test_parse_float() {
        let json = Bson::Double(1.0).into_psql_json().to_string();
        assert_eq!(r#"{"$f":"1.0"}"#, json);
    }

    #[test]
    fn test_parse_datetime() {
        let date = chrono::DateTime::parse_from_rfc3339("1996-12-19T16:39:57-08:00").unwrap();
        let time: u128 = SystemTime::from(date)
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let json = Bson::DateTime(bson::DateTime::from_millis(time.try_into().unwrap()))
            .into_psql_json()
            .to_string();
        assert_eq!(r#"{"$d":851042397000}"#, json);
    }

    #[test]
    fn test_parse_object_id() {
        let json =
            Bson::ObjectId(bson::oid::ObjectId::parse_str("62c75f564f084cd855b6ac3f").unwrap())
                .into_psql_json()
                .to_string();
        assert_eq!(r#"{"$o":"62c75f564f084cd855b6ac3f"}"#, json);
    }

    #[test]
    fn test_parse_javascript() {
        let json = Bson::JavaScriptCode("function a() { return 'hey'; }".to_string())
            .into_psql_json()
            .to_string();
        assert_eq!(r#"{"$j":"function a() { return 'hey'; }"}"#, json);
    }

    #[test]
    fn test_parse_javascript_with_scope() {
        let json = Bson::JavaScriptCodeWithScope(bson::JavaScriptCodeWithScope {
            code: "function a() { return 'hey'; }".to_string(),
            scope: doc! { "a": 1, "b": 2 },
        })
        .into_psql_json()
        .to_string();
        assert_eq!(
            r#"{"$j":"function a() { return 'hey'; }","s":"{\"a\":1,\"b\":2}"}"#,
            json
        );
    }

    #[test]
    fn test_parse_regex() {
        let json = Bson::RegularExpression(bson::Regex {
            pattern: "^[a-z]+$".to_string(),
            options: "i".to_string(),
        })
        .into_psql_json()
        .to_string();
        assert_eq!(r#"{"$r":"^[a-z]+$","o":"i"}"#, json);
    }
}
