use crate::{
    deserializer::PostgresJsonDeserializer,
    mutation,
    parser::{self, UpdateOper},
    serializer::{PostgresSerializer, STORAGE_VERSION},
    sql::{identifier, literal, Sql},
};
use bson::{Bson, Document};
use eyre::{bail, eyre, Result};
use postgres::{error::SqlState, types::ToSql, NoTls, Row};
use r2d2::PooledConnection;
use r2d2_postgres::PostgresConnectionManager;
use sha2::{Digest, Sha256};
use std::fmt;

pub const MAX_QUERY_ROWS: usize = 10_000;
pub const MAX_QUERY_BYTES: usize = 8 * 1024 * 1024;
const SCHEMA_VERSION: &str = "oxide:database:v1";

#[derive(Debug)]
pub enum CreateTableError {
    AlreadyExists(String),
    Other(eyre::Report),
}
#[derive(Debug, Default)]
pub struct UpdateResult {
    pub matched: u64,
    pub modified: u64,
    pub before: Option<Document>,
    pub after: Option<Document>,
    pub upserted: Option<Bson>,
}

pub struct PgDb {
    client: PooledConnection<PostgresConnectionManager<NoTls>>,
}
impl PgDb {
    pub fn try_from_pool(pool: &r2d2::Pool<PostgresConnectionManager<NoTls>>) -> Result<Self> {
        let mut client = pool.get()?;
        client.batch_execute("SET statement_timeout = '30s'; SET lock_timeout = '5s'")?;
        Ok(Self { client })
    }
    pub fn try_with_uri(uri: &str) -> Result<Self> {
        let manager = PostgresConnectionManager::new(uri.parse()?, NoTls);
        let pool = r2d2::Pool::builder().max_size(4).build(manager)?;
        Self::try_from_pool(&pool)
    }
    pub fn exec(&mut self, query: &str, params: &[&(dyn ToSql + Sync)]) -> Result<u64> {
        Ok(self.client.execute(query, params)?)
    }
    pub fn raw_query(&mut self, query: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>> {
        Ok(self.client.query(query, params)?)
    }
    pub fn query_one(
        &mut self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>> {
        Ok(self.raw_query(query, params)?.into_iter().next())
    }
    pub fn query(
        &mut self,
        query: &str,
        sp: SqlParam,
        filter: Option<Document>,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>> {
        self.require_collection(&sp)?;
        if !params.is_empty() {
            bail!("query parameters must be supplied by the filter compiler");
        }
        let filter = parser::compile(&filter.unwrap_or_default())?;
        let query = format!(
            "{} WHERE {}",
            query.replace("%table%", &sp.sanitize()),
            filter.text
        );
        Ok(self.client.query(&query, &filter.refs())?)
    }

    pub fn find(
        &mut self,
        sp: &SqlParam,
        filter: &Document,
        sort: Option<&Document>,
        skip: i64,
        limit: i64,
    ) -> Result<Vec<Document>> {
        self.require_collection(sp)?;
        let mut sql = parser::compile(filter)?;
        let order = order_by(sort, &mut sql)?;
        let cap = if limit > 0 {
            limit.min(MAX_QUERY_ROWS as i64)
        } else {
            MAX_QUERY_ROWS as i64
        };
        sql.text = format!(
            "SELECT _jsonb FROM {} WHERE {}{} LIMIT {} OFFSET {}",
            sp.sanitize(),
            sql.text,
            order,
            cap + 1,
            skip
        );
        self.documents(
            &sql,
            if limit > 0 && limit <= MAX_QUERY_ROWS as i64 {
                Some(limit as usize)
            } else {
                None
            },
        )
    }

    pub fn documents(&mut self, sql: &Sql, limit: Option<usize>) -> Result<Vec<Document>> {
        use postgres::fallible_iterator::FallibleIterator;
        let refs = sql.refs();
        let mut rows = self.client.query_raw(&sql.text, refs)?;
        let mut docs = Vec::new();
        let mut bytes = 0;
        while let Some(row) = rows.next()? {
            if limit.is_some_and(|limit| docs.len() == limit) {
                break;
            }
            if docs.len() == MAX_QUERY_ROWS {
                bail!("query exceeds the supported limit of {MAX_QUERY_ROWS} documents; use a selective filter or limit");
            }
            let value: serde_json::Value = row.try_get(0)?;
            let value = value.to_bson()?;
            let doc = value
                .as_document()
                .ok_or_else(|| eyre!("query returned a non-document"))?
                .clone();
            bytes += bson::to_vec(&doc)?.len();
            if bytes > MAX_QUERY_BYTES {
                bail!("query exceeds the supported result byte limit");
            }
            docs.push(doc);
        }
        Ok(docs)
    }

    pub fn delete(
        &mut self,
        sp: &SqlParam,
        filter: Option<&Document>,
        limit: Option<i32>,
    ) -> Result<u64> {
        self.require_collection(sp)?;
        let sql = parser::compile(&filter.cloned().unwrap_or_default())?;
        let limit = match limit.unwrap_or(0) {
            0 => String::new(),
            1 => " LIMIT 1".into(),
            _ => bail!("delete limit must be 0 or 1"),
        };
        let query = format!(
            "DELETE FROM {} WHERE ctid IN (SELECT ctid FROM {} WHERE {}{} FOR UPDATE)",
            sp.sanitize(),
            sp.sanitize(),
            sql.text,
            limit
        );
        Ok(self.client.execute(&query, &sql.refs())?)
    }

    pub fn find_and_remove(
        &mut self,
        sp: &SqlParam,
        filter: &Document,
        sort: Option<&Document>,
    ) -> Result<Option<Document>> {
        if !sp.exists(self)? {
            return Ok(None);
        }
        let mut sql = parser::compile(filter)?;
        let order = order_by(sort, &mut sql)?;
        let query = format!("DELETE FROM {sp} WHERE ctid IN (SELECT ctid FROM {sp} WHERE {}{order} LIMIT 1 FOR UPDATE) RETURNING _jsonb", sql.text);
        self.client
            .query_opt(&query, &sql.refs())?
            .map(|row| {
                let value: serde_json::Value = row.try_get(0)?;
                value
                    .to_bson()?
                    .as_document()
                    .cloned()
                    .ok_or_else(|| eyre!("invalid stored document"))
            })
            .transpose()
    }

    pub fn update(
        &mut self,
        sp: &SqlParam,
        filter: Option<&Document>,
        sort: Option<&Document>,
        update: UpdateOper,
        upsert: bool,
        multi: bool,
    ) -> Result<UpdateResult> {
        if multi && matches!(update, UpdateOper::Replace(_)) {
            bail!("replacement updates cannot be multi");
        }
        let mut sql = parser::compile(&filter.cloned().unwrap_or_default())?;
        let order = order_by(sort, &mut sql)?;
        if !sp.exists(self)? {
            if !upsert {
                return Ok(UpdateResult::default());
            }
            self.create_table_if_not_exists(&sp.db, &sp.collection)?;
        }
        let limit = if multi { MAX_QUERY_ROWS + 1 } else { 1 };
        let query = format!(
            "SELECT ctid::text, _jsonb FROM {} WHERE {}{} LIMIT {} FOR UPDATE",
            sp.sanitize(),
            sql.text,
            order,
            limit
        );
        let mut transaction = self.client.transaction()?;
        transaction
            .batch_execute("SET LOCAL statement_timeout = '30s'; SET LOCAL lock_timeout = '5s'")?;
        // Stream and bound the locked preimages before applying any changes.
        // Dropping the row iterator releases its mutable borrow, not row locks.
        let mut selected = Vec::new();
        let mut bytes = 0;
        {
            use postgres::fallible_iterator::FallibleIterator;
            let mut rows = transaction.query_raw(&query, sql.refs())?;
            while let Some(row) = rows.next()? {
                if selected.len() == MAX_QUERY_ROWS {
                    bail!("update exceeds supported document limit");
                }
                let tid: String = row.try_get(0)?;
                let json: serde_json::Value = row.try_get(1)?;
                let before = json
                    .to_bson()?
                    .as_document()
                    .ok_or_else(|| eyre!("invalid stored document"))?
                    .clone();
                bytes += bson::to_vec(&before)?.len();
                if bytes > MAX_QUERY_BYTES {
                    bail!("update exceeds supported byte limit");
                }
                selected.push((tid, before));
            }
        }
        let mut result = UpdateResult::default();
        for (tid, before) in selected {
            let after = mutation::apply(&before, &update)?;
            validate_document(&after)?;
            result.matched += 1;
            if after != before {
                let json = Bson::Document(after.clone()).into_psql_json();
                let id = identity(after.get("_id").expect("validated identity"))?;
                transaction.execute(
                    &format!(
                        "UPDATE {} SET _jsonb=$1, _id=$2 WHERE ctid=$3::text::tid",
                        sp.sanitize()
                    ),
                    &[&json, &id, &tid],
                )?;
                result.modified += 1;
            }
            if result.before.is_none() {
                result.before = Some(before);
                result.after = Some(after);
            }
        }
        if result.matched == 0 && upsert {
            let seed = mutation::upsert_seed(filter)?;
            let after = mutation::apply(&seed, &update)?;
            validate_document(&after)?;
            let id = after.get("_id").expect("validated identity");
            transaction.execute(
                &format!("INSERT INTO {} (_jsonb, _id) VALUES ($1,$2)", sp.sanitize()),
                &[
                    &Bson::Document(after.clone()).into_psql_json(),
                    &identity(id)?,
                ],
            )?;
            result.upserted = Some(id.clone());
            result.after = Some(after);
        }
        transaction.commit()?;
        Ok(result)
    }

    pub fn insert_doc(&mut self, sp: SqlParam, doc: &Document) -> Result<Document> {
        self.require_collection(&sp)?;
        let mut doc = doc.clone();
        if !doc.contains_key("_id") {
            doc.insert("_id", bson::oid::ObjectId::new());
        }
        validate_document(&doc)?;
        let id = identity(doc.get("_id").expect("validated identity"))?;
        self.exec(
            &format!("INSERT INTO {} (_jsonb, _id) VALUES ($1,$2)", sp.sanitize()),
            &[&Bson::Document(doc.clone()).into_psql_json(), &id],
        )?;
        Ok(doc)
    }
    pub fn insert_docs(&mut self, sp: SqlParam, docs: &mut Vec<Document>) -> Result<u64> {
        let mut count = 0;
        for doc in docs {
            *doc = self.insert_doc(sp.clone(), doc)?;
            count += 1;
        }
        Ok(count)
    }
    pub fn table_exists(&mut self, db: &str, collection: &str) -> Result<bool> {
        let sp = SqlParam::new(db, collection);
        sp.validate()?;
        let exists: bool = self
            .client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_tables WHERE schemaname=$1 AND tablename=$2)",
                &[&db, &collection],
            )?
            .get(0);
        if exists {
            self.require_collection(&sp)?;
        }
        Ok(exists)
    }
    pub fn require_collection(&mut self, sp: &SqlParam) -> Result<()> {
        sp.validate()?;
        let row = self.client.query_opt("SELECT obj_description(c.oid, 'pg_class') FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=$2 AND c.relkind='r'", &[&sp.db,&sp.collection])?;
        let version: Option<String> = row
            .ok_or_else(|| eyre!("collection does not exist"))?
            .get(0);
        if version.as_deref() != Some(STORAGE_VERSION) {
            bail!("collection {} has an unversioned or unsupported storage format; export with the old version and import into a new collection", sp);
        }
        Ok(())
    }
    pub fn get_strings(
        &mut self,
        query: String,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<String>> {
        self.raw_query(&query, params)?
            .into_iter()
            .map(|r| Ok(r.try_get(0)?))
            .collect()
    }
    pub fn get_schemas(&mut self) -> Result<Vec<String>> {
        self.get_strings("SELECT nspname FROM pg_namespace WHERE obj_description(oid,'pg_namespace')=$1 ORDER BY nspname".into(), &[&SCHEMA_VERSION])
    }
    pub fn get_tables(&mut self, schema: &str) -> Result<Vec<String>> {
        self.get_strings("SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relkind='r' AND obj_description(c.oid,'pg_class')=$2 ORDER BY c.relname".into(), &[&schema,&STORAGE_VERSION])
    }
    pub fn get_table_indexes(&mut self, schema: &str, table: &str) -> Result<Vec<Row>> {
        self.require_collection(&SqlParam::new(schema, table))?;
        self.raw_query("SELECT indexname, indexdef, obj_description((quote_ident(schemaname)||'.'||quote_ident(indexname))::regclass,'pg_class') AS definition FROM pg_indexes WHERE schemaname=$1 AND tablename=$2", &[&schema,&table])
    }
    pub fn get_table_size(&mut self, schema: &str, table: &str) -> Result<i64> {
        self.client
            .query_one(
                "SELECT pg_relation_size($1::text::regclass)",
                &[&SqlParam::new(schema, table).sanitize()],
            )
            .map(|r| r.get(0))
            .map_err(Into::into)
    }
    pub fn create_db_if_not_exists(&mut self, db: &str) -> Result<u64> {
        validate_name(db)?;
        match self
            .client
            .execute(&format!("CREATE DATABASE {}", identifier(db)), &[])
        {
            Ok(n) => Ok(n),
            Err(e) if matches!(e.code(),Some(c) if c == &SqlState::DUPLICATE_DATABASE || c == &SqlState::UNIQUE_VIOLATION) => {
                Ok(0)
            }
            Err(e) => Err(e.into()),
        }
    }
    pub fn create_schema_if_not_exists(&mut self, schema: &str) -> Result<u64> {
        validate_name(schema)?;
        let mut tx = self.client.transaction()?;
        tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtext($1))",
            &[&format!("oxide-schema:{schema}")],
        )?;
        if let Some(row) = tx.query_opt(
            "SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE nspname=$1",
            &[&schema],
        )? {
            let version: Option<String> = row.get(0);
            if version.as_deref() != Some(SCHEMA_VERSION) {
                bail!("schema is not owned by this Oxide storage version");
            }
        } else {
            tx.batch_execute(&format!(
                "CREATE SCHEMA {}; COMMENT ON SCHEMA {} IS {}",
                identifier(schema),
                identifier(schema),
                literal(SCHEMA_VERSION)
            ))?;
        }
        tx.commit()?;
        Ok(0)
    }
    pub fn create_table(&mut self, sp: SqlParam) -> Result<u64, CreateTableError> {
        if self
            .table_exists(&sp.db, &sp.collection)
            .map_err(CreateTableError::Other)?
        {
            return Err(CreateTableError::AlreadyExists(sp.to_string()));
        }
        self.create_table_if_not_exists(&sp.db, &sp.collection)
            .map_err(CreateTableError::Other)
    }
    pub fn create_table_if_not_exists(&mut self, schema: &str, table: &str) -> Result<u64> {
        let sp = SqlParam::new(schema, table);
        sp.validate()?;
        self.create_schema_if_not_exists(schema)?;
        let mut tx = self.client.transaction()?;
        tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtext($1))",
            &[&format!("oxide-table:{schema}:{table}")],
        )?;
        let exists: bool = tx
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_tables WHERE schemaname=$1 AND tablename=$2)",
                &[&schema, &table],
            )?
            .get(0);
        if !exists {
            let index_name = physical_index(&sp, "_id_");
            tx.batch_execute(&format!("CREATE TABLE {} (_jsonb jsonb NOT NULL, _id jsonb NOT NULL); CREATE UNIQUE INDEX {} ON {} (_id); COMMENT ON TABLE {} IS {}; COMMENT ON INDEX {}.{} IS {}", sp,identifier(&index_name),sp,sp,literal(STORAGE_VERSION),identifier(schema),identifier(&index_name),literal(&serde_json::json!({"name":"_id_","key":{"_id":1},"unique":true}).to_string())))?;
        }
        tx.commit()?;
        self.require_collection(&sp)?;
        Ok(0)
    }
    pub fn create_index(&mut self, sp: &SqlParam, index: &Document) -> Result<u64> {
        self.require_collection(sp)?;
        let key = index.get_document("key")?;
        if key.is_empty() {
            bail!("index key cannot be empty");
        }
        for option in index.keys() {
            if !["key", "name", "unique", "v", "background"].contains(&option.as_str()) {
                bail!("unsupported index option {option}");
            }
        }
        let name = index.get_str("name")?;
        validate_name(name)?;
        let unique = optional_bool(index, "unique", false)?;
        let definition = serde_json::json!({"name":name,"key":key,"unique":unique}).to_string();
        let physical = physical_index(sp, name);
        let mut tx = self.client.transaction()?;
        tx.query_one("SELECT pg_advisory_xact_lock(hashtext($1))", &[&physical])?;
        if let Some(row) = tx.query_opt("SELECT obj_description(c.oid,'pg_class') FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=$2", &[&sp.db,&physical])? {
            let existing: Option<String> = row.get(0);
            if existing.as_deref() != Some(&definition) { bail!("index name already exists with a different definition"); }
            return Ok(0);
        }
        let mut fields = Vec::new();
        let mut checks = Vec::new();
        for (field, direction) in key {
            let direction = integer(direction)?;
            if direction != 1 && direction != -1 {
                bail!("only ascending/descending indexes are supported");
            }
            let value = field_sql(field)?;
            if unique {
                // Multikey unique indexes need different semantics. A CHECK
                // constraint rejects arrays now and on future writes atomically.
                let mut prefix = String::new();
                for part in field.split('.') {
                    if !prefix.is_empty() {
                        prefix.push('.');
                    }
                    prefix.push_str(part);
                    checks.push(format!(
                        "jsonb_typeof({}) IS DISTINCT FROM 'array'",
                        field_sql(&prefix)?
                    ));
                }
                checks.push(format!("(jsonb_typeof({value}) IS DISTINCT FROM 'object' OR {value} ?| ARRAY['$i','$f','$o','$d','$b'])"));
            }
            fields.push(format!(
                "(COALESCE({},'null'::jsonb)) {}",
                identity_expr(&value),
                if direction == 1 { "ASC" } else { "DESC" }
            ));
        }
        if unique {
            tx.batch_execute(&format!(
                "ALTER TABLE {sp} ADD CONSTRAINT {} CHECK ({})",
                identifier(&physical),
                checks.join(" AND ")
            ))?;
        }
        tx.batch_execute(&format!(
            "CREATE {} INDEX {} ON {} ({}); COMMENT ON INDEX {}.{} IS {}",
            if unique { "UNIQUE" } else { "" },
            identifier(&physical),
            sp,
            fields.join(","),
            identifier(&sp.db),
            identifier(&physical),
            literal(&definition)
        ))?;
        tx.commit()?;
        Ok(0)
    }
    pub fn drop_db(&mut self, db: &str) -> Result<u64> {
        validate_name(db)?;
        self.exec(
            &format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", identifier(db)),
            &[],
        )
    }
    pub fn drop_schema(&mut self, schema: &str) -> Result<u64> {
        validate_name(schema)?;
        let row = self.client.query_opt(
            "SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE nspname=$1",
            &[&schema],
        )?;
        if let Some(row) = row {
            let version: Option<String> = row.get(0);
            if version.as_deref() != Some(SCHEMA_VERSION) {
                bail!("refusing to drop a schema not owned by Oxide");
            }
            self.exec(&format!("DROP SCHEMA {} CASCADE", identifier(schema)), &[])
        } else {
            Ok(0)
        }
    }
    pub fn drop_table(&mut self, sp: &SqlParam) -> Result<u64> {
        if !self.table_exists(&sp.db, &sp.collection)? {
            return Ok(0);
        }
        self.exec(&format!("DROP TABLE {}", sp), &[])
    }
    pub fn schema_stats(&mut self, schema: &str, collection: Option<&str>) -> Result<Row> {
        Ok(self.client.query_one("SELECT COUNT(*)::integer AS TableCount, COALESCE(SUM(s.n_live_tup),0)::bigint AS RowCount, COALESCE(SUM(pg_total_relation_size(c.oid)),0)::bigint AS TotalSize, COALESCE(SUM(pg_indexes_size(c.oid)),0)::bigint AS IndexSize, COALESCE(SUM(pg_relation_size(c.oid)),0)::bigint AS RelationSize, COALESCE(SUM((SELECT COUNT(*) FROM pg_index i WHERE i.indrelid=c.oid)),0)::integer AS IndexCount FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_stat_user_tables s ON s.relid=c.oid WHERE n.nspname=$1 AND c.relkind='r' AND ($2::text IS NULL OR c.relname=$2) AND obj_description(c.oid,'pg_class')=$3", &[&schema,&collection,&STORAGE_VERSION])?)
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
        Self {
            db: db.into(),
            collection: collection.into(),
            comment: None,
        }
    }
    pub fn from(doc: &Document, col_attr: &str) -> Result<Self> {
        let sp = Self::new(doc.get_str("$db")?, doc.get_str(col_attr)?);
        sp.validate()?;
        Ok(sp)
    }
    pub fn validate(&self) -> Result<()> {
        validate_name(&self.db)?;
        validate_name(&self.collection)
    }
    pub fn sanitize(&self) -> String {
        format!("{}.{}", identifier(&self.db), identifier(&self.collection))
    }
    pub fn exists(&self, client: &mut PgDb) -> Result<bool> {
        client.table_exists(&self.db, &self.collection)
    }
}
impl fmt::Display for SqlParam {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.sanitize())
    }
}
fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > 63 || name.contains('\0') {
        bail!("names must be 1..63 bytes and contain no NUL");
    }
    Ok(())
}
fn physical_index(sp: &SqlParam, name: &str) -> String {
    format!(
        "ox_{:x}",
        Sha256::digest(format!("{}\0{}\0{name}", sp.db, sp.collection).as_bytes())
    )[..63]
        .to_string()
}
pub fn integer(value: &Bson) -> Result<i64> {
    match value {
        Bson::Int32(i) => Ok(*i as i64),
        Bson::Int64(i) => Ok(*i),
        _ => bail!("expected an integer"),
    }
}
pub fn optional_bool(doc: &Document, key: &str, default: bool) -> Result<bool> {
    match doc.get(key) {
        None => Ok(default),
        Some(Bson::Boolean(b)) => Ok(*b),
        _ => bail!("{key} must be a boolean"),
    }
}
pub fn field_sql(field: &str) -> Result<String> {
    if field.is_empty() || field.contains('\0') {
        bail!("invalid field path");
    }
    Ok(format!(
        "_jsonb{}",
        field
            .split('.')
            .map(|p| format!("->{}", literal(&crate::serializer::encode_key(p))))
            .collect::<String>()
    ))
}
pub fn order_by(sort: Option<&Document>, sql: &mut Sql) -> Result<String> {
    let mut terms = Vec::new();
    if let Some(sort) = sort {
        for (field, direction) in sort {
            let direction = integer(direction)?;
            if direction != 1 && direction != -1 {
                bail!("sort direction must be 1 or -1");
            }
            let path = field
                .split('.')
                .map(crate::serializer::encode_key)
                .collect::<Vec<_>>();
            let param = sql.bind(serde_json::to_string(&path)?, "jsonb");
            let value = format!("_jsonb #> ARRAY(SELECT jsonb_array_elements_text({param}))");
            terms.push(format!(
                "{} {} NULLS {}",
                identity_expr(&value),
                if direction == 1 { "ASC" } else { "DESC" },
                if direction == 1 { "FIRST" } else { "LAST" }
            ));
        }
    }
    Ok(if terms.is_empty() {
        String::new()
    } else {
        format!(" ORDER BY {}", terms.join(","))
    })
}
pub fn identity_expr(value: &str) -> String {
    format!("CASE WHEN {value} ? '$i' THEN (({value}->>'$i')::numeric)::text::jsonb WHEN {value}->>'$f'='-0' THEN '0'::jsonb WHEN jsonb_typeof({value}->'$f')='number' THEN (({value}->>'$f')::numeric)::text::jsonb ELSE {value} END")
}
fn identity(value: &Bson) -> Result<serde_json::Value> {
    Ok(match value {
        Bson::Array(_) | Bson::RegularExpression(_) => bail!("unsupported _id type"),
        Bson::Double(f) if !f.is_finite() => bail!("nonfinite _id is unsupported"),
        Bson::Int64(i) => serde_json::json!(i),
        Bson::Double(f) => serde_json::json!(f),
        Bson::Document(d) => {
            let mut out = serde_json::Map::new();
            for (k, v) in d {
                out.insert(crate::serializer::encode_key(k), identity(v)?);
            }
            serde_json::Value::Object(out)
        }
        _ => value.clone().into_psql_json(),
    })
}
fn validate_document(doc: &Document) -> Result<()> {
    identity(doc.get("_id").ok_or_else(|| eyre!("missing _id"))?)?;
    if bson::to_vec(doc)?.len() > crate::wire::MAX_DOCUMENT_LEN as usize {
        bail!("document exceeds BSON size limit");
    }
    Ok(())
}
