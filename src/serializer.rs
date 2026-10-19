//! Version-two JSONB representation with explicit BSON document field order.
//! User keys beginning with `$` are escaped so that native BSON tags can never
//! collide with an ordinary document.
use bson::{doc, Bson};
use serde_json::{json, Value};

pub const STORAGE_VERSION: &str = "oxide:jsonb:v2";

pub fn encode_key(key: &str) -> String {
    if key.starts_with('$') {
        format!("${key}")
    } else {
        key.to_owned()
    }
}

pub trait PostgresSerializer {
    fn into_psql_json(self) -> Value;
}

impl PostgresSerializer for Bson {
    fn into_psql_json(self) -> Value {
        match self {
            Bson::Int32(i) => json!(i),
            Bson::Int64(i) => json!({"$i": i.to_string()}),
            Bson::Double(f) if f.is_finite() && !(f == 0.0 && f.is_sign_negative()) => {
                json!({"$f": f})
            }
            Bson::Double(f) => json!({"$f": f.to_string()}),
            Bson::DateTime(d) => json!({"$d": d.timestamp_millis()}),
            Bson::ObjectId(id) => json!({"$o": id.to_hex()}),
            Bson::String(s) if !s.contains('\0') => json!(s),
            Bson::Boolean(b) => json!(b),
            Bson::Null => Value::Null,
            Bson::Array(a) => Value::Array(a.into_iter().map(Self::into_psql_json).collect()),
            Bson::Document(d) => {
                let keys = d.keys().map(|k| Value::String(encode_key(k))).collect();
                let mut object: serde_json::Map<String, Value> = d
                    .into_iter()
                    .map(|(k, v)| (encode_key(&k), v.into_psql_json()))
                    .collect();
                object.insert("$k".into(), Value::Array(keys));
                Value::Object(object)
            }
            // BSON's own encoding preserves binary subtypes, Decimal128 payloads,
            // regex options and every remaining scalar without an ExtJSON ambiguity.
            other => json!({"$b": bson::to_vec(&doc! {"v": other}).expect("valid BSON scalar")}),
        }
    }
}
