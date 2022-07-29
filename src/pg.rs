use crate::commands::{InvalidUpdateError, UpdateDoc, UpdateOper};
use crate::parser::value_to_jsonb;
use crate::serializer::PostgresSerializer;
use crate::utils::{collapse_fields, expand_fields};
use bson::{Bson, Document};
use eyre::{eyre, Result};
use indoc::indoc;
use postgres::error::{Error, SqlState};
use postgres::{types::ToSql, NoTls, Row};
use r2d2::PooledConnection;
use r2d2_postgres::PostgresConnectionManager;
use sql_lexer::sanitize_string;
use std::env;
use std::error::Error as StdError;
use std::fmt;

#[derive(Debug)]
pub struct AlreadyExistsError {
    _target: String,
}

impl std::fmt::Display for AlreadyExistsError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Table {} already exists", self._target)
    }
}

impl StdError for AlreadyExistsError {}

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
        PgDb::new_with_uri(&env::var("DATABASE_URL").unwrap())
    }

    pub fn new_from_pool(pool: r2d2::Pool<PostgresConnectionManager<NoTls>>) -> Self {
        let client = pool.get().unwrap();
        PgDb { client }
    }

    pub fn new_with_uri(uri: &str) -> Self {
        let manager = PostgresConnectionManager::new(uri.parse().unwrap(), NoTls);
        let pool = r2d2::Pool::new(manager).unwrap();
        let client = pool.get().unwrap();
        PgDb { client }
    }

    pub fn exec(&mut self, query: &str, params: &[&(dyn ToSql + Sync)]) -> Result<u64> {
        log::debug!("SQL: {} - {:#?}", query, params);
        match self.client.execute(query, params) {
            Ok(rows) => Ok(rows),
            Err(e) => Err(eyre! {e}),
        }
    }

    pub fn query(
        &mut self,
        query: &str,
        sp: SqlParam,
        filter: Option<Document>,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>> {
        let mut sql = self.get_query(query, sp);

        if let Some(f) = filter {
            let filter_doc = expand_fields(&f).unwrap();
            let filter = super::parser::parse(filter_doc)?;
            if filter != "" {
                sql = format!("{} WHERE {}", sql, filter);
            }
        }

        log::debug!("SQL: {} - {:#?}", sql, params);
        self.raw_query(&sql, params)
    }

    fn get_matching_ids(
        &mut self,
        sp: &SqlParam,
        limit: i32,
        filter: Option<&Document>,
    ) -> Result<Option<Vec<String>>> {
        let where_str = match filter {
            Some(f) => {
                let filter = super::parser::parse(f.clone())?;
                if filter != "" {
                    format!("WHERE {}", filter)
                } else {
                    "".to_string()
                }
            }
            None => "".to_string(),
        };
        let sql = format!(
            "SELECT _jsonb->'_id'->>'$o' FROM {} {} LIMIT {}",
            sp.sanitize(),
            where_str,
            limit,
        );
        let ids = self
            .client
            .query(&sql, &[])
            .unwrap()
            .into_iter()
            .map(|r| r.get(0))
            .collect::<Vec<String>>();
        if ids.is_empty() {
            Ok(None)
        } else {
            Ok(Some(ids))
        }
    }

    fn add_ids_to_where(&mut self, sp: &SqlParam, limit: i32, where_str: &str) -> Result<String> {
        let ids = self.get_matching_ids(sp, limit, None)?;
        if ids.is_some() {
            let in_ids = format!(
                "({})",
                ids.unwrap()
                    .into_iter()
                    .map(|id| format!("'{}'", id))
                    .collect::<Vec<String>>()
                    .join(", ")
            );
            let where_id = format!("_jsonb->'_id'->>'$o' IN {}", in_ids);
            if where_str == "" {
                Ok(format!("WHERE {}", where_id))
            } else {
                Ok(format!("{} AND {}", where_str, where_id))
            }
        } else {
            Ok(where_str.to_string())
        }
    }

    pub fn delete(
        &mut self,
        sp: &SqlParam,
        filter: Option<&Document>,
        limit: Option<i32>,
    ) -> Result<u64> {
        let mut where_str = if let Some(f) = filter {
            if f.keys().count() < 1 {
                "".to_string()
            } else {
                format!(" WHERE {}", super::parser::parse(f.clone())?)
            }
        } else {
            "".to_string()
        };

        // apply limit
        if let Some(limit) = limit {
            if limit > 0 {
                where_str = self.add_ids_to_where(sp, limit, where_str.as_str())?;
            }
        }

        let sql = format!("DELETE FROM {}{}", sp.to_string(), where_str);
        self.exec(&sql, &[])
    }

    pub fn update(
        &mut self,
        sp: &SqlParam,
        filter: Option<&Document>,
        update: UpdateOper,
        upsert: bool,
        multi: bool,
    ) -> Result<u64> {
        let mut where_str = if let Some(f) = filter {
            if f.keys().count() < 1 {
                "".to_string()
            } else {
                format!(" WHERE {}", super::parser::parse(f.clone())?)
            }
        } else {
            "".to_string()
        };

        let table_name = format!("{}", sp.sanitize());
        if !multi {
            // gets the first id that matches
            let sql = format!(
                "SELECT _jsonb->'_id'->>'$o' FROM {} {} LIMIT 1",
                table_name, where_str
            );
            let rows = self.raw_query(&sql, &[]).unwrap();
            if !upsert && rows.len() < 1 {
                return Ok(0);
            }
            if rows.len() > 0 {
                let id: String = rows[0].get(0);
                let match_id = format!("_jsonb->'_id'->>'$o' = '{}'", id);
                if where_str == "" {
                    where_str = format!(" WHERE {}", match_id);
                } else {
                    where_str = format!("{} AND {}", where_str, match_id);
                }
            }
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

                            let sql = format!("UPDATE {} SET {}{}", table_name, updates, where_str);
                            statements.push(sql);
                        }
                        UpdateDoc::Unset(unset) => {
                            let mut removals = vec![];
                            let fields = collapse_fields(&unset);

                            for field in fields.keys().filter(|f| !f.contains(".")) {
                                removals.push(format!(" - '{}'", field));
                            }

                            for field in fields.keys().filter(|f| f.contains(".")) {
                                removals.push(format!(" #- '{{{}}}'", field.replace(".", ",")));
                            }

                            let sql = format!(
                                "UPDATE {} SET _jsonb = _jsonb{}{}",
                                table_name,
                                removals.join(""),
                                where_str
                            );
                            statements.push(sql);
                        }
                        UpdateDoc::Inc(inc) => {
                            let updates = inc
                                .iter()
                                .map(|(k, v)|
                                    format!("json_build_object('{}', COALESCE(_jsonb->'{}')::numeric + {})::jsonb", k, k, v)
                                )
                                .collect::<Vec<String>>()
                                .join(" || ");

                            // UPDATE "test"."ages" SET
                            // 	_jsonb = _jsonb
                            // 		|| json_build_object('age', COALESCE(_jsonb->'age')::numeric + 1)::jsonb
                            // 		|| json_build_object('limit', COALESCE(_jsonb->'limit')::numeric - 2)::jsonb;

                            let sql = format!(
                                indoc! {"
                                UPDATE {}
                                SET _jsonb = _jsonb ||
                                    {}
                                {}
                            "},
                                table_name, updates, where_str
                            );
                            statements.push(sql);
                        }
                    }
                }
                statements
            }
            UpdateOper::Replace(mut replace) => {
                if !replace.contains_key("_id") {
                    replace.insert("_id", bson::oid::ObjectId::new());
                }
                let json = Bson::Document(replace).into_psql_json();
                let needs_insert = upsert
                    && !{
                        let query = format!(
                            "SELECT EXISTS(SELECT _jsonb FROM {} {} LIMIT 1)",
                            table_name, where_str
                        );
                        let row = self.client.query_one(&query, &[]).unwrap();
                        let exists: bool = row.get(0);
                        exists
                    };
                let sql = if needs_insert {
                    format!("INSERT INTO {} (_jsonb) VALUES ($1)", table_name)
                } else {
                    format!("UPDATE {} SET _jsonb = $1 {}", table_name, where_str)
                };
                return Ok(self.exec(&sql, &[&json])?);
            }
        };

        // FIXME start a transaction here
        // #16 - https://github.com/fcoury/oxide/issues/16
        let mut total = 0;
        for sql in statements {
            total += self.exec(&sql, &[])?;
        }
        Ok(total)
    }

    pub fn raw_query(&mut self, query: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>> {
        log::debug!("SQL: {} - {:#?}", query, params);
        match self.client.query(query, params) {
            Ok(rows) => Ok(rows),
            Err(e) => Err(eyre! {e}),
        }
    }

    fn get_query(&self, s: &str, sp: SqlParam) -> String {
        let table = sp.sanitize();
        sanitize_string(s.replace("%table%", &table))
    }

    pub fn insert_docs(&mut self, sp: SqlParam, docs: &mut Vec<Document>) -> Result<u64> {
        let query = self.get_query("INSERT INTO %table% VALUES ($1)", sp);

        let mut affected = 0;
        for doc in docs {
            if !doc.contains_key("_id") {
                doc.insert("_id", bson::oid::ObjectId::new());
            }
            let bson: Bson = Bson::Document(doc.clone()).into();
            let json = bson.into_psql_json();
            let n = &self.exec(&query, &[&json]).unwrap();
            affected += n;
        }
        Ok(affected)
    }

    pub fn table_exists(&mut self, db: &str, collection: &str) -> Result<bool> {
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
    ) -> Result<Vec<String>> {
        let mut strings = Vec::new();
        let rows = self.raw_query(&sql, params).unwrap();
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

    pub fn get_table_indexes(&mut self, schema: &str, table: &str) -> Result<Vec<Row>> {
        self.raw_query(
            "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = $1 AND tablename = $2",
            &[
                &sanitize_string(schema.to_string()),
                &sanitize_string(table.to_string()),
            ],
        )
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

    pub fn create_db_if_not_exists(&mut self, db: &str) -> Result<u64> {
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
            return Err(eyre! {err});
        }
        Ok(res?)
    }

    pub fn create_schema_if_not_exists(&mut self, schema: &str) -> Result<u64> {
        let sql = format!(
            r#"CREATE SCHEMA IF NOT EXISTS "{}""#,
            sanitize_string(schema.to_string())
        );

        let mut attempt = 0;
        loop {
            match self.client.execute(&sql, &[]) {
                Ok(u64) => return Ok(u64),
                Err(err) => {
                    if let Some(sql_state) = err.code() {
                        log::info!("Error {:?} - attempt {}", sql_state, attempt);
                        if sql_state != &SqlState::DUPLICATE_DATABASE
                            && sql_state != &SqlState::UNIQUE_VIOLATION
                        {
                            return Err(eyre! {err});
                        }
                    }
                    attempt += 1;
                    if attempt > 3 {
                        return Err(eyre! {err});
                    }
                }
            }
        }
    }

    pub fn create_table(&mut self, sp: SqlParam) -> Result<u64, CreateTableError> {
        let query = format!("CREATE TABLE {} (_jsonb jsonb)", sp.clone());
        match self.client.execute(&query, &[]) {
            Ok(u64) => Ok(u64),
            Err(err) => {
                if let Some(sql_state) = err.code() {
                    if sql_state == &SqlState::DUPLICATE_TABLE
                        || sql_state == &SqlState::UNIQUE_VIOLATION
                    {
                        return Err(CreateTableError::AlreadyExists(AlreadyExistsError {
                            _target: sp.to_string(),
                        }));
                    }
                }
                Err(CreateTableError::Other(err))
            }
        }
    }

    pub fn create_table_if_not_exists(&mut self, schema: &str, table: &str) -> Result<u64> {
        let name = SqlParam::new(schema, table).sanitize();
        let sql = format!("CREATE TABLE IF NOT EXISTS {} (_jsonb jsonb)", name);

        self.create_schema_if_not_exists(schema).unwrap();

        let mut attempt = 0;
        loop {
            match self.client.execute(&sql, &[]) {
                Ok(u64) => return Ok(u64),
                Err(err) => {
                    if let Some(sql_state) = err.code() {
                        log::info!("Error {:?} - attempt {}", sql_state, attempt);
                        if sql_state != &SqlState::DUPLICATE_DATABASE
                            && sql_state != &SqlState::UNIQUE_VIOLATION
                        {
                            return Err(eyre! {err});
                        }
                    }
                    attempt += 1;
                    if attempt > 3 {
                        return Err(eyre! {err});
                    }
                }
            }
        }
    }

    pub fn create_index(&mut self, sp: &SqlParam, index: &Document) -> Result<u64> {
        let fields: Vec<String> = index
            .get_document("key")
            .unwrap()
            .into_iter()
            .map(|(k, _)| format!("(_jsonb->'{}')", k))
            .collect();
        let unique = if index.get_bool("unique").unwrap_or(false) {
            " UNIQUE"
        } else {
            ""
        };
        let name = index.get_str("name").unwrap();
        let sql = format!(
            "CREATE{} INDEX {} ON {} ({})",
            unique,
            name,
            sp.sanitize(),
            fields.join(", ")
        );

        self.exec(&sql, &[])
    }

    pub fn drop_db(&mut self, schema: &str) -> Result<u64> {
        let sql = format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", schema);
        self.exec(&sql, &[])
    }

    pub fn drop_schema(&mut self, schema: &str) -> Result<u64> {
        let sql = format!("DROP SCHEMA IF EXISTS {} CASCADE", schema);
        self.exec(&sql, &[])
    }

    pub fn drop_table(&mut self, sp: &SqlParam) -> Result<u64> {
        let name = sp.sanitize();
        let sql = format!("DROP TABLE IF EXISTS {}", name);
        self.exec(&sql, &[])
    }

    pub fn schema_stats(&mut self, schema: &str, collection: Option<&str>) -> Result<Row> {
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

        match self.client.query_one(&sql, &[]) {
            Ok(row) => Ok(row),
            Err(err) => Err(eyre!("Error retrieving schema stats: {}", err)),
        }
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

    pub fn exists(&self, client: &mut PgDb) -> Result<bool> {
        client.table_exists(&self.db, &self.collection)
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
