use bson::{ser, Bson};
use chrono::{TimeZone, Utc};
use serde_json::Value;

pub trait PostgresJsonDeserializer {
    fn from_psql_json(&self) -> Bson;
}

impl PostgresJsonDeserializer for Value {
    fn from_psql_json(&self) -> Bson {
        match self {
            serde_json::Value::String(s) => Bson::String(s.to_string()),
            serde_json::Value::Number(n) => {
                let s = n.to_string();
                if s.contains(".") {
                    Bson::Double(n.as_f64().unwrap())
                } else {
                    if let Some(n) = n.as_i64() {
                        Bson::Int32(n.try_into().unwrap())
                    } else if let Some(n) = n.as_f64() {
                        Bson::Double(n)
                    } else {
                        panic!("Unsupported number type while attempting to deserialize Value::Number for {}", n);
                    }
                }
            }
            serde_json::Value::Bool(b) => Bson::Boolean(b.to_owned()),
            serde_json::Value::Null => Bson::Null,
            serde_json::Value::Array(a) => {
                Bson::Array(a.into_iter().map(|v| v.from_psql_json()).collect())
            }
            serde_json::Value::Object(o) => {
                if o.contains_key("$o") {
                    return Bson::ObjectId(
                        bson::oid::ObjectId::parse_str(o["$o"].as_str().unwrap().to_string())
                            .unwrap(),
                    );
                }
                if o.contains_key("$d") {
                    return Bson::DateTime(Utc.timestamp_millis(o["$d"].as_i64().unwrap()).into());
                }
                if o.contains_key("$f") {
                    return Bson::Double(o["$f"].as_f64().unwrap());
                }
                if o.contains_key("$j") {
                    if o.contains_key("s") {
                        return Bson::JavaScriptCodeWithScope(bson::JavaScriptCodeWithScope {
                            code: o["$j"].as_str().unwrap().to_string(),
                            scope: ser::to_document(&o["s"]).unwrap(),
                        });
                    } else {
                        return Bson::JavaScriptCode(o["$j"].as_str().unwrap().to_string());
                    }
                }
                if o.contains_key("$r") {
                    return Bson::RegularExpression(bson::Regex {
                        pattern: o["$r"].as_str().unwrap().to_string(),
                        options: o["o"].as_str().unwrap().to_string(),
                    });
                }
                let mut m = bson::Document::new();
                for (k, v) in o {
                    m.insert(k, v.from_psql_json());
                }
                Bson::Document(m)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_date() {
        let json = r#"{"$d":1546300800000}"#;
        let bson: serde_json::Value = serde_json::from_str(json).unwrap();
        let bson = bson.from_psql_json();
        println!("{:?}", bson);
        assert_eq!(bson, Bson::DateTime(Utc.timestamp(1546300800, 0).into()));
    }
}
