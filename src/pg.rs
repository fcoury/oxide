use crate::serializer::PostgresSerializer;
use bson::Bson;
use postgres::error::Error;
use postgres::Row;
use postgres::{types::ToSql, Client, NoTls};
use sql_lexer::sanitize_string;
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

    pub fn query(
        &mut self,
        query: &str,
        sp: SqlParam,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, Error> {
        let sql = self.get_query(query, sp);

        println!("*** SQL: {} - {:#?}", sql, params);
        self.client.query(&sql, params)
    }

    fn get_query(&self, s: &str, sp: SqlParam) -> String {
        let table = format!("{}.{}", &sp.db, &sp.collection);
        sanitize_string(s.replace("%table%", &table))
    }

    pub fn insert_docs(&mut self, sp: SqlParam, docs: &Vec<Bson>) -> Result<u64, Error> {
        let query = self.get_query("INSERT INTO %table% VALUES ($1)", sp);

        let mut affected = 0;
        for doc in docs {
            let bson: Bson = doc.into();
            let json = bson.into_psql_json();
            let n = &self.exec(&query, &[&json]).unwrap();
            affected += n;
        }
        Ok(affected)
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

    pub fn create_schema_if_not_exists(&mut self, schema: &str) -> Result<u64, Error> {
        let sql = format!(
            "CREATE SCHEMA IF NOT EXISTS {}",
            sanitize_string(schema.to_string())
        );
        self.exec(&sql, &[])
    }

    pub fn create_table_if_not_exists(&mut self, schema: &str, table: &str) -> Result<u64, Error> {
        let name = SqlParam::new(schema, table).sanitize();
        let sql = format!("CREATE TABLE IF NOT EXISTS {} (_jsonb jsonb)", name);

        self.create_schema_if_not_exists(schema)?;
        self.exec(&sql, &[])
    }
}

pub struct SqlParam {
    pub db: String,
    pub collection: String,
    pub comment: Option<String>,
}

impl SqlParam {
    pub fn new(db: &str, collection: &str) -> Self {
        SqlParam {
            db: db.to_string(),
            collection: collection.to_string(),
            comment: None,
        }
    }

    pub fn sanitize(&self) -> String {
        let db = sanitize_string(self.db.to_string());
        let collection = sanitize_string(self.collection.to_string());
        format!("{}.{}", db, collection)
    }
}
