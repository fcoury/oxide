use crate::deserializer::PostgresJsonDeserializer;
use crate::parser::{value_to_jsonb, InvalidUpdateError, UpdateDoc, UpdateOper};
use crate::serializer::PostgresSerializer;
use crate::utils::{collapse_fields, expand_fields};
use bson::{Bson, Document};
use eyre::{eyre, Result};
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
pub enum UpdateResult {
    Count(u64),
    Document(Document),
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

    pub fn query_one(
        &mut self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>> {
        let rows = self.raw_query(query, params)?;
        let row = rows.into_iter().next();
        Ok(row)
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

        println!("SQL: {} - {:#?}", sql, params);
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
        sort: Option<&Document>,
        update: UpdateOper,
        upsert: bool,
        multi: bool,
        returning: bool,
    ) -> Result<UpdateResult> {
        let mut where_str = if let Some(f) = filter {
            if f.keys().count() < 1 {
                "".to_string()
            } else {
                format!(" WHERE {}", super::parser::parse(f.clone())?)
            }
        } else {
            "".to_string()
        };

        let return_str = if returning { " RETURNING _jsonb" } else { "" };

        let table_name = format!("{}", sp.sanitize());
        if !multi {
            let order_by_str = get_order_by(sort);
            // gets the first id that matches
            let sql = format!(
                "SELECT _jsonb->'_id' FROM {} {}{} LIMIT 1",
                table_name, where_str, order_by_str
            );
            let rows = self.raw_query(&sql, &[]).unwrap();
            if !upsert && rows.len() < 1 {
                return Ok(UpdateResult::Count(0));
            }
            if rows.len() > 0 {
                let id: serde_json::Value = rows[0].get(0);
                let match_id = format!("_jsonb->'_id' = '{}'", id);
                if where_str == "" {
                    where_str = format!(" WHERE {}", match_id);
                } else {
                    where_str = format!("{} AND {}", where_str, match_id);
                }
            }
        };

        if let UpdateOper::Update(updates) = update.clone() {
            self.check_preconditions(&sp, &updates)?;
        }

        let statements = match update {
            UpdateOper::Update(updates) => updates
                .iter()
                .map(|u| {
                    format!(
                        "UPDATE {} SET {}{}{}",
                        table_name,
                        update_from_operation(u),
                        where_str,
                        return_str
                    )
                })
                .collect::<Vec<String>>(),
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
                    format!(
                        "INSERT INTO {} (_jsonb) VALUES ($1){}",
                        table_name, return_str
                    )
                } else {
                    format!(
                        "UPDATE {} SET _jsonb = $1 {}{}",
                        table_name, where_str, return_str
                    )
                };

                return if returning {
                    let res = self.raw_query(&sql, &[&json])?;
                    println!("{:?}", res);
                    // Ok(UpdateResult::Document(res))
                    todo!("Not ready yet")
                } else {
                    let res = self.exec(&sql, &[&json])?;
                    Ok(UpdateResult::Count(res))
                };
            }
        };

        if returning {
            let sql = statements[0].clone();
            let res = self.raw_query(&sql, &[])?;
            let row = res.get(0).unwrap();
            let json: serde_json::Value = row.get(0);
            let doc = json.from_psql_json();

            Ok(UpdateResult::Document(doc.as_document().unwrap().clone()))
        } else {
            // FIXME start a transaction here
            // #16 - https://github.com/fcoury/oxide/issues/16
            let mut total = 0;
            for sql in statements {
                total += self.exec(&sql, &[])?;
            }
            Ok(UpdateResult::Count(total))
        }
    }

    pub fn check_preconditions(&mut self, sp: &SqlParam, updates: &Vec<UpdateDoc>) -> Result<()> {
        for update in updates {
            match update {
                UpdateDoc::AddToSet(add_to_set) => {
                    // check if any of the fields to be set is not an array
                    let mut checks = vec![];
                    for (field, _) in add_to_set.iter() {
                        checks.push(format!("jsonb_typeof(_jsonb->'{}') <> 'array'", field));
                    }
                    let where_str = checks.join(" OR ");
                    let table = sp.sanitize();
                    let sql = format!("SELECT _jsonb->'_id' AS _id FROM {table} WHERE {where_str}");
                    let row = self.query_one(&sql, &[])?;
                    if let Some(row) = row {
                        let id: serde_json::Value = row.get(0);
                        let id = id.from_psql_json();
                        let field = add_to_set.keys().next().unwrap();
                        let err = format!(
                            "Cannot apply $addToSet to a non-array field. Field named '{}' has a non-array type int in the document _id: {}",
                            field, id
                        );
                        return Err(eyre! { err });
                    }
                }
                _ => {}
            }
        }
        Ok(())
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

    pub fn insert_doc(&mut self, sp: SqlParam, doc: &Document) -> Result<Document> {
        let query = self.get_query("INSERT INTO %table% VALUES ($1) RETURNING _jsonb", sp);
        let mut doc = doc.clone();

        if !doc.contains_key("_id") {
            doc.insert("_id", bson::oid::ObjectId::new());
        }

        let bson: Bson = Bson::Document(doc.clone()).into();
        let json = bson.into_psql_json();
        let res = self.raw_query(&query, &[&json])?;
        let row = res.get(0).unwrap();
        let json: serde_json::Value = row.get(0);
        let doc = json.from_psql_json();

        Ok(doc.as_document().unwrap().clone())
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
            .map(|(k, _)| {
                format!(
                    "(_jsonb->{})",
                    k.split(".")
                        .into_iter()
                        .map(|k| format!("'{}'", k))
                        .collect::<Vec<_>>()
                        .join("->")
                )
            })
            .collect();
        let unique = if index.get_bool("unique").unwrap_or(false) {
            " UNIQUE"
        } else {
            ""
        };
        let name = index.get_str("name").unwrap().replace(".", "_");
        let sql = format!(
            "CREATE{} INDEX IF NOT EXISTS {} ON {} ({})",
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

fn update_from_operation(update: &UpdateDoc) -> String {
    match update {
        UpdateDoc::Set(set) => set
            .keys()
            .map(|k| {
                let field = format!("_jsonb['{}']", sanitize_string(k.clone()));
                let value = value_to_jsonb(set.get(k).unwrap());
                format!("{} = '{}'", field, value)
            })
            .collect::<Vec<String>>()
            .join(", "),
        UpdateDoc::Unset(unset) => {
            let mut removals = vec![];

            let fields = collapse_fields(&unset);

            for field in fields.keys().filter(|f| !f.contains(".")) {
                removals.push(format!(" - '{}'", field));
            }

            for field in fields.keys().filter(|f| f.contains(".")) {
                removals.push(format!(" #- '{{{}}}'", field.replace(".", ",")));
            }

            format!("_jsonb = _jsonb{}", removals.join(""))
        }
        UpdateDoc::Inc(inc) => format!(
            "_jsonb = _jsonb || {}",
            inc.iter()
                .map(|(k, v)| format!(
                    "json_build_object('{}', COALESCE(_jsonb->'{}')::numeric + {})::jsonb",
                    k, k, v
                ))
                .collect::<Vec<String>>()
                .join(" || ")
        ),
        UpdateDoc::AddToSet(add_to_set) => {
            let mut current = "_jsonb".to_string();
            for (field, value) in add_to_set.iter() {
                current = format!("jsonb_set({current}, '{{{field}}}', CASE WHEN NOT _jsonb ? '{field}' THEN '[{value}]' WHEN NOT _jsonb->'{field}' @> '{value}' THEN _jsonb->'{field}' || '{value}' ELSE _jsonb->'{field}' END)");
            }
            format!("_jsonb = {}", current)
        }
    }
}

pub fn get_where(filter: Option<&Document>) -> Option<String> {
    if let Some(f) = filter {
        if f.keys().count() < 1 {
            None
        } else {
            Some(super::parser::parse(f.clone()).unwrap())
        }
    } else {
        None
    }
}

pub fn get_order_by(sort: Option<&Document>) -> String {
    if let Some(s) = sort {
        let mut order_by = vec![];
        for (k, v) in s.iter() {
            let v = if v.as_i32().unwrap() > 0 {
                "ASC"
            } else {
                "DESC"
            };
            order_by.push(format!("_jsonb->'{}' {}", k, v));
        }
        format!(" ORDER BY {}", order_by.join(", "))
    } else {
        "".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    #[test]
    fn test_update_set() {
        let doc = UpdateDoc::Set(doc! {
            "a": 1,
            "b": 2,
        });
        assert_eq!(
            update_from_operation(&doc),
            "_jsonb['a'] = '1', _jsonb['b'] = '2'"
        );
    }

    #[test]
    fn test_update_unset() {
        let doc = UpdateDoc::Unset(doc! {
            "a": 1,
            "b": 1,
        });
        assert_eq!(update_from_operation(&doc), "_jsonb = _jsonb - 'a' - 'b'");
    }

    #[test]
    fn test_update_inc() {
        let doc = UpdateDoc::Inc(doc! {
            "a": 1,
            "b": 3,
        });
        assert_eq!(update_from_operation(&doc), "_jsonb = _jsonb || json_build_object('a', COALESCE(_jsonb->'a')::numeric + 1)::jsonb || json_build_object('b', COALESCE(_jsonb->'b')::numeric + 3)::jsonb");
    }

    #[test]
    fn test_update_add_to_set() {
        let doc = UpdateDoc::AddToSet(doc! {
            "letters": "a",
            "colors": "red",
        });
        assert_eq!(
            update_from_operation(&doc),
            r#"_jsonb = jsonb_set(jsonb_set(_jsonb, '{letters}', CASE WHEN NOT _jsonb ? 'letters' THEN '["a"]' WHEN NOT _jsonb->'letters' @> '"a"' THEN _jsonb->'letters' || '"a"' ELSE _jsonb->'letters' END), '{colors}', CASE WHEN NOT _jsonb ? 'colors' THEN '["red"]' WHEN NOT _jsonb->'colors' @> '"red"' THEN _jsonb->'colors' || '"red"' ELSE _jsonb->'colors' END)"#
        );
    }

    #[test]
    fn test_get_where() {
        let doc = doc! {
            "a": "1",
            "b": "2",
        };
        assert_eq!(
            "(_jsonb->'a' = '\"1\"' AND _jsonb->'b' = '\"2\"')",
            get_where(Some(&doc)).unwrap()
        );
    }

    #[test]
    fn test_get_order_by() {
        let doc = doc! {
            "a": 1,
            "b": -1,
        };
        assert_eq!(
            " ORDER BY _jsonb->'a' ASC, _jsonb->'b' DESC",
            get_order_by(Some(&doc))
        );
    }
}
