//! SQL parameters are always text with explicit server-side casts. Identifiers
//! are quoted separately because PostgreSQL cannot bind them as values.
use postgres::types::ToSql;
use postgres_protocol::escape::{escape_identifier, escape_literal};

pub fn identifier(name: &str) -> String {
    escape_identifier(name)
}
pub fn literal(value: &str) -> String {
    escape_literal(value)
}

#[derive(Default, Debug)]
pub struct Sql {
    pub text: String,
    pub params: Vec<String>,
}

impl Sql {
    pub fn bind(&mut self, value: impl ToString, cast: &str) -> String {
        self.params.push(value.to_string());
        format!("${}::text::{cast}", self.params.len())
    }
    pub fn refs(&self) -> Vec<&(dyn ToSql + Sync)> {
        self.params
            .iter()
            .map(|v| v as &(dyn ToSql + Sync))
            .collect()
    }
}
