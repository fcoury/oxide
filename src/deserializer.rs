use bson::Bson;
use eyre::{bail, eyre, Result};
use serde_json::Value;

pub trait PostgresJsonDeserializer {
    fn to_bson(&self) -> Result<Bson>;
}

impl PostgresJsonDeserializer for Value {
    fn to_bson(&self) -> Result<Bson> {
        Ok(match self {
            Value::String(s) => Bson::String(s.clone()),
            Value::Bool(b) => Bson::Boolean(*b),
            Value::Null => Bson::Null,
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    i32::try_from(i).map(Bson::Int32).unwrap_or(Bson::Int64(i))
                } else {
                    Bson::Double(n.as_f64().ok_or_else(|| eyre!("invalid number"))?)
                }
            }
            Value::Array(a) => Bson::Array(a.iter().map(Self::to_bson).collect::<Result<_>>()?),
            Value::Object(o) if o.len() == 1 && o.contains_key("$i") => Bson::Int64(
                o["$i"]
                    .as_str()
                    .ok_or_else(|| eyre!("invalid int64 tag"))?
                    .parse()?,
            ),
            Value::Object(o) if o.len() == 1 && o.contains_key("$f") => {
                Bson::Double(match &o["$f"] {
                    Value::Number(n) => n.as_f64().ok_or_else(|| eyre!("invalid double tag"))?,
                    Value::String(s) => s.parse()?,
                    _ => bail!("invalid double tag"),
                })
            }
            Value::Object(o) if o.len() == 1 && o.contains_key("$d") => {
                Bson::DateTime(bson::DateTime::from_millis(
                    o["$d"].as_i64().ok_or_else(|| eyre!("invalid date tag"))?,
                ))
            }
            Value::Object(o) if o.len() == 1 && o.contains_key("$o") => {
                Bson::ObjectId(bson::oid::ObjectId::parse_str(
                    o["$o"]
                        .as_str()
                        .ok_or_else(|| eyre!("invalid ObjectId tag"))?,
                )?)
            }
            Value::Object(o) if o.len() == 1 && o.contains_key("$b") => {
                let bytes: Vec<u8> = serde_json::from_value(o["$b"].clone())?;
                let mut d: bson::Document = bson::from_slice(&bytes)?;
                d.remove("v").ok_or_else(|| eyre!("invalid BSON tag"))?
            }
            Value::Object(o) => {
                let mut doc = bson::Document::new();
                for (key, value) in o {
                    if key.starts_with('$') && !key.starts_with("$$") {
                        bail!("unknown storage tag {key}");
                    }
                    doc.insert(
                        key.strip_prefix("$$")
                            .map(|k| format!("${k}"))
                            .unwrap_or_else(|| key.clone()),
                        value.to_bson()?,
                    );
                }
                Bson::Document(doc)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serializer::PostgresSerializer;
    use bson::{doc, spec::BinarySubtype, Binary, Regex};

    #[test]
    fn bson_round_trips_without_tag_collisions() {
        let values = vec![
            Bson::Int32(1),
            Bson::Int64(1),
            Bson::Int64(i64::MAX),
            Bson::Double(1.5),
            Bson::Double(f64::INFINITY),
            Bson::DateTime(bson::DateTime::from_millis(i64::MIN)),
            Bson::ObjectId(bson::oid::ObjectId::new()),
            Bson::RegularExpression(Regex {
                pattern: "a'".into(),
                options: "im".into(),
            }),
            Bson::Binary(Binary {
                subtype: BinarySubtype::Uuid,
                bytes: vec![0; 16],
            }),
            Bson::Decimal128(bson::Decimal128::from_bytes([7; 16])),
            Bson::Timestamp(bson::Timestamp {
                time: 5,
                increment: 2,
            }),
            Bson::Document(
                doc! {"$o": "not-an-id", "$$i": 1, "$numberLong": "2", "nested": [{"$f": "text"}]},
            ),
            Bson::JavaScriptCodeWithScope(bson::JavaScriptCodeWithScope {
                code: "x".into(),
                scope: doc! {"$o": "literal"},
            }),
            Bson::MinKey,
            Bson::MaxKey,
        ];
        for value in values {
            assert_eq!(value.clone().into_psql_json().to_bson().unwrap(), value);
        }
        assert!(
            matches!(Bson::Double(f64::NAN).into_psql_json().to_bson().unwrap(), Bson::Double(f) if f.is_nan())
        );
    }

    #[test]
    fn malformed_tags_return_errors() {
        for value in [
            serde_json::json!({"$i": "bad"}),
            serde_json::json!({"$o":"bad"}),
            serde_json::json!({"$b":[256]}),
        ] {
            assert!(value.to_bson().is_err());
        }
    }
}
