#![allow(dead_code)]
use crate::commands::{UpdateDoc, UpdateOper};
use crate::parser::value_to_jsonb;
use crate::serializer::PostgresSerializer;
use bson::{Bson, Document};
use postgres::error::{Error, SqlState};
use postgres::{types::ToSql, NoTls, Row};
use r2d2::PooledConnection;
use r2d2_postgres::PostgresConnectionManager;
use sql_lexer::sanitize_string;
use std::env;
use std::fmt;

#[derive(Debug)]
pub struct AlreadyExistsError {
    target: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InvalidUpdateError {
    reason: String,
}

impl InvalidUpdateError {
    pub fn new(reason: String) -> Self {
        InvalidUpdateError { reason }
    }
}

#[derive(Debug)]
pub enum CreateTableError {
    AlreadyExists(AlreadyExistsError),
    Other(Error),
}

#[derive(Debug)]
pub enum UpdateError {
    InvalidUpdate(InvalidUpdateError),
    Other(Error),
}

pub struct PgDb {
    client: PooledConnection<PostgresConnectionManager<NoTls>>,
}

impl PgDb {
    pub fn new() -> Self {
        PgDb::new_with_uri(env::var("DATABASE_URL").unwrap())
    }

    pub fn new_from_pool(pool: r2d2::Pool<PostgresConnectionManager<NoTls>>) -> Self {
        let client = pool.get().unwrap();
        PgDb { client }
    }

    pub fn new_with_uri(uri: String) -> Self {
        let manager = PostgresConnectionManager::new(uri.parse().unwrap(), NoTls);
        let pool = r2d2::Pool::new(manager).unwrap();
        let client = pool.get().unwrap();
        PgDb { client }
    }

    pub fn exec(&mut self, query: &str, params: &[&(dyn ToSql + Sync)]) -> Result<u64, Error> {
        log::debug!("SQL: {} - {:#?}", query, params);
        self.client.execute(query, params)
    }

    pub fn query(
        &mut self,
        query: &str,
        sp: SqlParam,
        filter: Option<Document>,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, Error> {
        let mut sql = self.get_query(query, sp);

        if let Some(f) = filter {
            let filter = super::parser::parse(f);
            if filter != "" {
                sql = format!("{} WHERE {}", sql, filter);
            }
        }

        log::debug!("SQL: {} - {:#?}", sql, params);
        self.raw_query(&sql, params)
    }

    pub fn update(
        &mut self,
        sp: &SqlParam,
        filter: Option<&Document>,
        update: UpdateOper,
        _multi: bool,
    ) -> Result<u64, UpdateError> {
        let where_str = if let Some(f) = filter {
            if f.keys().count() < 1 {
                "".to_string()
            } else {
                format!(" WHERE {}", super::parser::parse(f.clone()))
            }
        } else {
            "".to_string()
        };

        let statements = match update {
            UpdateOper::Update(updates) => {
                let mut statements = vec![];
                for update in updates {
                    match update {
                        UpdateDoc::Set(set) => {
                            let updates = set
                                .keys()
                                .map(|k| {
                                    let field = format!("_jsonb['{}']", sanitize_string(k.clone()));
                                    let value = value_to_jsonb(format!("{}", set.get(k).unwrap()));
                                    format!("{} = {}", field, value)
                                })
                                .collect::<Vec<String>>()
                                .join(", ");

                            let sql =
                                format!("UPDATE {} SET {}{}", sp.sanitize(), updates, where_str);
                            statements.push(sql);
                        }
                        UpdateDoc::Inc(_inc) => {
                            todo!()
                        }
                    }
                }
                statements
            }
            UpdateOper::Replace(_replace) => {
                todo!()
            }
        };

        // FIXME start a transaction here
        let mut total = 0;
        for sql in statements {
            match self.exec(&sql, &[]) {
                Ok(n) => total += n,
                Err(e) => {
                    log::error!("Error trying to update: {:?}", e);
                    return Err(UpdateError::Other(e));
                }
            }
        }
        Ok(total)
    }

    pub fn raw_query(
        &mut self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, Error> {
        log::debug!("SQL: {} - {:#?}", query, params);
        self.client.query(query, params)
    }

    fn get_query(&self, s: &str, sp: SqlParam) -> String {
        let table = sp.sanitize();
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

    pub fn table_exists(&mut self, db: &str, collection: &str) -> Result<bool, Error> {
        let query = format!("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_tables WHERE schemaname = '{}' AND tablename = '{}')", db, collection);
        let rows = self.client.query(&query, &[]).unwrap();
        let row = rows.get(0).unwrap();
        let exists: bool = row.get(0);
        Ok(exists)
    }

    pub fn get_strings(
        &mut self,
        sql: String,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<String>, Error> {
        let mut strings = Vec::new();
        let rows = self.raw_query(&sql, params)?;
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
        let schema_table = SqlParam::new(schema, table).sanitize();
        let row = self
            .client
            .query_one(
                format!("SELECT pg_relation_size('{}')", schema_table).as_str(),
                &[],
            )
            .unwrap();

        row.get(0)
    }

    pub fn create_db_if_not_exists(&mut self, db: &str) -> Result<u64, Error> {
        let query = format!("CREATE DATABASE {}", db);
        let res = self.client.execute(&query, &[]);
        if let Err(err) = res {
            if let Some(sql_state) = err.code() {
                if sql_state == &SqlState::DUPLICATE_DATABASE
                    || sql_state == &SqlState::UNIQUE_VIOLATION
                {
                    return Ok(0);
                }
            }
            return Err(err);
        }
        res
    }

    pub fn create_schema_if_not_exists(&mut self, schema: &str) -> Result<u64, Error> {
        let sql = format!(
            "CREATE SCHEMA IF NOT EXISTS {}",
            sanitize_string(schema.to_string())
        );

        let mut attempt = 0;
        loop {
            match self.exec(&sql, &[]) {
                Ok(u64) => return Ok(u64),
                Err(err) => {
                    if let Some(sql_state) = err.code() {
                        log::info!("Error {:?} - attempt {}", sql_state, attempt);
                        if sql_state != &SqlState::DUPLICATE_DATABASE
                            && sql_state != &SqlState::UNIQUE_VIOLATION
                        {
                            return Err(err);
                        }
                    }
                    attempt += 1;
                    if attempt > 3 {
                        return Err(err);
                    }
                }
            }
        }
    }

    pub fn create_table(&mut self, sp: SqlParam) -> Result<u64, CreateTableError> {
        let query = format!("CREATE TABLE {} (_jsonb jsonb)", sp.clone());
        match self.exec(&query, &[]) {
            Ok(u64) => Ok(u64),
            Err(err) => {
                if let Some(sql_state) = err.code() {
                    if sql_state == &SqlState::DUPLICATE_TABLE
                        || sql_state == &SqlState::UNIQUE_VIOLATION
                    {
                        return Err(CreateTableError::AlreadyExists(AlreadyExistsError {
                            target: sp.to_string(),
                        }));
                    }
                }
                Err(CreateTableError::Other(err))
            }
        }
    }

    pub fn create_table_if_not_exists(&mut self, schema: &str, table: &str) -> Result<u64, Error> {
        let name = SqlParam::new(schema, table).sanitize();
        let sql = format!("CREATE TABLE IF NOT EXISTS {} (_jsonb jsonb)", name);

        self.create_schema_if_not_exists(schema)?;

        let mut attempt = 0;
        loop {
            match self.exec(&sql, &[]) {
                Ok(u64) => return Ok(u64),
                Err(err) => {
                    if let Some(sql_state) = err.code() {
                        log::info!("Error {:?} - attempt {}", sql_state, attempt);
                        if sql_state != &SqlState::DUPLICATE_DATABASE
                            && sql_state != &SqlState::UNIQUE_VIOLATION
                        {
                            return Err(err);
                        }
                    }
                    attempt += 1;
                    if attempt > 3 {
                        return Err(err);
                    }
                }
            }
        }
    }

    pub fn drop_db(&mut self, schema: &str) -> Result<u64, Error> {
        let sql = format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", schema);
        self.exec(&sql, &[])
    }

    pub fn drop_schema(&mut self, schema: &str) -> Result<u64, Error> {
        let sql = format!("DROP SCHEMA IF EXISTS {} CASCADE", schema);
        self.exec(&sql, &[])
    }

    pub fn drop_table(&mut self, sp: &SqlParam) -> Result<u64, Error> {
        let name = sp.sanitize();
        let sql = format!("DROP TABLE IF EXISTS {}", name);
        self.exec(&sql, &[])
    }

    pub fn schema_stats(&mut self, schema: &str, collection: Option<&str>) -> Result<Row, Error> {
        let mut sql = format!(
            r#"
            SELECT COUNT(distinct t.table_name)::integer                                                          AS TableCount,
                COALESCE(SUM(s.n_live_tup), 0)::integer                                                           AS RowCount,
                COALESCE(SUM(pg_total_relation_size('"'||t.table_schema||'"."'||t.table_name||'"')), 0)::integer  AS TotalSize,
                COALESCE(SUM(pg_indexes_size('"'||t.table_schema||'"."'||t.table_name||'"')), 0)::integer         AS IndexSize,
                COALESCE(SUM(pg_relation_size('"'||t.table_schema||'"."'||t.table_name||'"')), 0)::integer        AS RelationSize,
                COUNT(distinct i.indexname)::integer                                                              AS IndexCount
            FROM information_schema.tables AS t
            LEFT OUTER
            JOIN pg_stat_user_tables       AS s ON s.schemaname = t.table_schema
                                                AND s.relname = t.table_name
            LEFT OUTER
            JOIN pg_indexes                AS i ON i.schemaname = t.table_schema
                                                AND i.tablename = t.table_name
            WHERE t.table_schema = '{}'
        "#,
            sanitize_string(schema.to_string())
        );

        if let Some(collection) = collection {
            sql = format!(
                "{} AND t.table_name = '{}'",
                sql,
                sanitize_string(collection.to_string())
            );
        }

        self.client.query_one(&sql, &[])
    }
}

#[derive(Debug, Clone)]
pub struct SqlParam {
    pub db: String,
    pub collection: String,
    pub comment: Option<String>,
}

impl SqlParam {
    pub fn new(db: &str, collection: &str) -> Self {
        SqlParam {
            db: sanitize_string(db.to_string()),
            collection: sanitize_string(collection.to_string()),
            comment: None,
        }
    }

    pub fn from(doc: &Document, col_attr: &str) -> Self {
        Self::new(
            &doc.get_str("$db").unwrap().to_string(),
            &doc.get_str(col_attr).unwrap().to_string(),
        )
    }

    pub fn sanitize(&self) -> String {
        format!(r#""{}"."{}""#, self.db, self.collection)
    }
}

impl fmt::Display for SqlParam {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.sanitize())
    }
}
