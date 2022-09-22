use chrono::DateTime;
use serde::Serialize;
use serde_json::Value;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TracerType {
    Db,
    None,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Trace {
    pub id: i32,
    pub input: Value,
    pub sql: String,
    pub params: String, // FIXME: Needs to be JSON
    pub created_at: DateTime<chrono::Utc>,
}
