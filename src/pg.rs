use postgres::error::Error;
use postgres::{types::ToSql, Client, NoTls};
use std::env;

pub struct PgDb {
    client: Client,
}

impl PgDb {
    pub fn new() -> Self {
        PgDb::new_with_uri(env::var("DATABASE_URL").unwrap())
    }

    pub fn new_with_uri(uri: String) -> Self {
        let client = Client::connect(&uri, NoTls).unwrap();
        PgDb { client }
    }

    pub fn exec(&mut self, query: &str, params: &[&(dyn ToSql + Sync)]) -> Result<u64, Error> {
        println!("*** SQL: {} - {:#?}", query, params);
        self.client.execute(query, params)
    }

    pub fn get_strings(
        &mut self,
        sql: String,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<String>, Error> {
        let mut strings = Vec::new();
        let rows = self.client.query(&sql, params)?;
        for row in rows.iter() {
            strings.push(row.get(0));
        }
        Ok(strings)
    }

    pub fn get_schemas(&mut self) -> Vec<String> {
        let schemas = self
            .get_strings(
                "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name"
                    .to_string(),
                &[],
            )
            .unwrap();
        schemas
            .into_iter()
            .filter(|s| !s.starts_with("pg_") && !(s == "information_schema"))
            .collect()
    }

    pub fn get_tables(&mut self, schema: &str) -> Vec<String> {
        self.get_strings(
            "
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = $1
                GROUP BY table_name ORDER BY table_name
                "
            .to_string(),
            &[&schema],
        )
        .unwrap()
    }

    pub fn get_table_size(&mut self, schema: &str, table: &str) -> i64 {
        let row = self
            .client
            .query_one(
                format!("SELECT pg_relation_size('{}.{}')", schema, table).as_str(),
                &[],
            )
            .unwrap();

        row.get(0)
    }
}
