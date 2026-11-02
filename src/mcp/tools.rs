//! Tool definitions and the command documents they hand to the wire dispatcher.
use crate::{
    cursor::Cursors,
    handler::{execute_command, CommandExecutionError},
    pg::integer,
};
use bson::{doc, Bson, Document};
use serde_json::{json, Map, Value};
use std::sync::Mutex;

use super::PgPool;

const MCP_BATCH: i32 = 10_000;
const DEFAULT_FIND_LIMIT: i64 = 20;
const MAX_FIND_LIMIT: i64 = 10_000;
const WRITE_TOOLS: &[&str] = &["insert", "update", "delete"];

pub(crate) fn is_write(name: &str) -> bool {
    WRITE_TOOLS.contains(&name)
}

pub(crate) fn definitions(allow_writes: bool) -> Vec<Value> {
    let mut tools = vec![
        tool(
            "list_databases",
            "List Oxide databases (PostgreSQL schemas owned by this storage version) and their sizes.",
            json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }),
            true,
            false,
        ),
        tool(
            "list_collections",
            "List collections in one database.",
            json!({
                "type": "object",
                "properties": {
                    "database": {"type": "string", "description": "Database name"}
                },
                "required": ["database"],
                "additionalProperties": false
            }),
            true,
            false,
        ),
        tool(
            "find",
            "Find documents. `limit` defaults to 20; pass 0 to use the server snapshot cap of 10000. Filters are MongoDB query documents.",
            json!({
                "type": "object",
                "properties": {
                    "database": {"type": "string"},
                    "collection": {"type": "string"},
                    "filter": {"type": "object", "description": "MongoDB query. Defaults to {}."},
                    "projection": {"type": "object"},
                    "sort": {"type": "object", "description": "Field to 1 or -1."},
                    "limit": {"type": "integer", "minimum": 0, "maximum": MAX_FIND_LIMIT},
                    "skip": {"type": "integer", "minimum": 0}
                },
                "required": ["database", "collection"],
                "additionalProperties": false
            }),
            true,
            false,
        ),
        tool(
            "aggregate",
            "Run an aggregation pipeline. Stages are Oxide's supported subset, including $match, $sort, $project, $group, $limit, and $lookup. Results are capped at 10000 documents.",
            json!({
                "type": "object",
                "properties": {
                    "database": {"type": "string"},
                    "collection": {"type": "string"},
                    "pipeline": {
                        "type": "array",
                        "items": {"type": "object"},
                        "description": "Aggregation stages, in order."
                    }
                },
                "required": ["database", "collection", "pipeline"],
                "additionalProperties": false
            }),
            true,
            false,
        ),
        tool(
            "count",
            "Count documents matching a query.",
            json!({
                "type": "object",
                "properties": {
                    "database": {"type": "string"},
                    "collection": {"type": "string"},
                    "query": {"type": "object"},
                    "limit": {"type": "integer", "minimum": 0},
                    "skip": {"type": "integer", "minimum": 0}
                },
                "required": ["database", "collection"],
                "additionalProperties": false
            }),
            true,
            true,
        ),
    ];
    if allow_writes {
        tools.extend([
            tool(
                "insert",
                "Insert documents. Disabled unless the server was started with --allow-writes.",
                json!({
                    "type": "object",
                    "properties": {
                        "database": {"type": "string"},
                        "collection": {"type": "string"},
                        "documents": {"type": "array", "items": {"type": "object"}, "minItems": 1}
                    },
                    "required": ["database", "collection", "documents"],
                    "additionalProperties": false
                }),
                false,
                false,
            ),
            tool(
                "update",
                "Update documents matching a filter. `update` is a replacement document or an operator document such as {$set: ...}. Disabled unless the server was started with --allow-writes.",
                json!({
                    "type": "object",
                    "properties": {
                        "database": {"type": "string"},
                        "collection": {"type": "string"},
                        "filter": {"type": "object"},
                        "update": {"type": "object"},
                        "multi": {"type": "boolean", "description": "Update every match. Defaults to false."},
                        "upsert": {"type": "boolean", "description": "Insert when nothing matches. Defaults to false."}
                    },
                    "required": ["database", "collection", "filter", "update"],
                    "additionalProperties": false
                }),
                false,
                true,
            ),
            tool(
                "delete",
                "Delete documents matching a filter. Defaults to one document; pass many: true to delete every match. Disabled unless the server was started with --allow-writes.",
                json!({
                    "type": "object",
                    "properties": {
                        "database": {"type": "string"},
                        "collection": {"type": "string"},
                        "filter": {"type": "object"},
                        "many": {"type": "boolean"}
                    },
                    "required": ["database", "collection", "filter"],
                    "additionalProperties": false
                }),
                false,
                true,
            ),
        ]);
    }
    tools
}

fn tool(
    name: &str,
    description: &str,
    input_schema: Value,
    read_only: bool,
    destructive: bool,
) -> Value {
    json!({
        "name": name,
        "description": description,
        "inputSchema": input_schema,
        "annotations": {
            "title": name,
            "readOnlyHint": read_only,
            "destructiveHint": destructive,
            "idempotentHint": read_only,
            "openWorldHint": true
        }
    })
}

pub(crate) fn call(pool: &PgPool, name: &str, arguments: &Value) -> Result<Value, String> {
    match name {
        "list_databases" => list_databases(pool, arguments),
        "list_collections" => list_collections(pool, arguments),
        "find" => find(pool, arguments),
        "aggregate" => aggregate(pool, arguments),
        "count" => count(pool, arguments),
        "insert" => insert(pool, arguments),
        "update" => update(pool, arguments),
        "delete" => delete(pool, arguments),
        other => Err(format!("unknown tool: {other}")),
    }
}

fn list_databases(pool: &PgPool, arguments: &Value) -> Result<Value, String> {
    let args = Args::new(arguments, &[])?;
    let _ = args;
    let reply = run(
        pool,
        doc! {"listDatabases": 1, "nameOnly": false, "$db": "admin"},
        None,
    )?;
    Ok(json!({
        "databases": bson_to_json(reply.get("databases").unwrap_or(&Bson::Array(vec![]))),
        "totalSize": number_field(&reply, "totalSize"),
        "totalSizeMb": number_field(&reply, "totalSizeMb"),
    }))
}

fn list_collections(pool: &PgPool, arguments: &Value) -> Result<Value, String> {
    let args = Args::new(arguments, &["database"])?;
    let database = args.string("database")?;
    let reply = run(
        pool,
        doc! {
            "listCollections": 1,
            "nameOnly": true,
            "cursor": {"batchSize": MCP_BATCH},
            "$db": &database,
        },
        Some((database.as_str(), "$cmd.listCollections")),
    )?;
    let names = reply
        .get_array("documents")
        .map_err(|error| error.to_string())?
        .iter()
        .map(|value| {
            value
                .as_document()
                .and_then(|doc| doc.get_str("name").ok())
                .map(|name| Value::String(name.to_owned()))
                .ok_or_else(|| "collection name missing".to_owned())
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(json!({"collections": names}))
}

fn find(pool: &PgPool, arguments: &Value) -> Result<Value, String> {
    let args = Args::new(
        arguments,
        &[
            "database",
            "collection",
            "filter",
            "projection",
            "sort",
            "limit",
            "skip",
        ],
    )?;
    let database = args.string("database")?;
    let collection = args.string("collection")?;
    let filter = args.document("filter")?.unwrap_or_default();
    let limit = args
        .bounded_i64("limit", 0, MAX_FIND_LIMIT)?
        .unwrap_or(DEFAULT_FIND_LIMIT);
    let skip = args.bounded_i64("skip", 0, i64::MAX)?.unwrap_or(0);
    let mut command = doc! {
        "find": &collection,
        "filter": filter,
        "skip": skip,
        "limit": limit,
        "batchSize": MCP_BATCH,
        "$db": &database,
    };
    if let Some(sort) = args.document("sort")? {
        command.insert("sort", sort);
    }
    if let Some(projection) = args.document("projection")? {
        command.insert("projection", projection);
    }
    let reply = run(
        pool,
        command,
        Some((database.as_str(), collection.as_str())),
    )?;
    Ok(json!({"documents": documents_json(&reply)?}))
}

fn aggregate(pool: &PgPool, arguments: &Value) -> Result<Value, String> {
    let args = Args::new(arguments, &["database", "collection", "pipeline"])?;
    let database = args.string("database")?;
    let collection = args.string("collection")?;
    let pipeline = args.pipeline()?;
    let reply = run(
        pool,
        doc! {
            "aggregate": &collection,
            "pipeline": pipeline,
            "cursor": {"batchSize": MCP_BATCH},
            "$db": &database,
        },
        Some((database.as_str(), collection.as_str())),
    )?;
    Ok(json!({"documents": documents_json(&reply)?}))
}

fn count(pool: &PgPool, arguments: &Value) -> Result<Value, String> {
    let args = Args::new(
        arguments,
        &["database", "collection", "query", "limit", "skip"],
    )?;
    let database = args.string("database")?;
    let collection = args.string("collection")?;
    let mut command = doc! {
        "count": &collection,
        "$db": &database,
    };
    if let Some(query) = args.document("query")? {
        command.insert("query", query);
    }
    if let Some(skip) = args.bounded_i64("skip", 0, i64::MAX)? {
        command.insert("skip", skip);
    }
    if let Some(limit) = args.bounded_i64("limit", 0, i64::MAX)? {
        command.insert("limit", limit);
    }
    let reply = run(pool, command, None)?;
    Ok(json!({"n": number_field(&reply, "n")}))
}

fn insert(pool: &PgPool, arguments: &Value) -> Result<Value, String> {
    let args = Args::new(arguments, &["database", "collection", "documents"])?;
    let database = args.string("database")?;
    let collection = args.string("collection")?;
    let documents = args.documents()?;
    let reply = run(
        pool,
        doc! {
            "insert": &collection,
            "documents": documents,
            "$db": &database,
        },
        None,
    )?;
    Ok(write_body(&reply))
}

fn update(pool: &PgPool, arguments: &Value) -> Result<Value, String> {
    let args = Args::new(
        arguments,
        &[
            "database",
            "collection",
            "filter",
            "update",
            "multi",
            "upsert",
        ],
    )?;
    let database = args.string("database")?;
    let collection = args.string("collection")?;
    let filter = args.document("filter")?.unwrap_or_default();
    let update = args
        .document("update")?
        .ok_or_else(|| "update is required".to_owned())?;
    let mut entry = doc! {"q": filter, "u": update};
    if let Some(multi) = args.bool("multi")? {
        entry.insert("multi", multi);
    }
    if let Some(upsert) = args.bool("upsert")? {
        entry.insert("upsert", upsert);
    }
    let reply = run(
        pool,
        doc! {
            "update": &collection,
            "updates": [entry],
            "$db": &database,
        },
        None,
    )?;
    Ok(write_body(&reply))
}

fn delete(pool: &PgPool, arguments: &Value) -> Result<Value, String> {
    let args = Args::new(arguments, &["database", "collection", "filter", "many"])?;
    let database = args.string("database")?;
    let collection = args.string("collection")?;
    let filter = args.document("filter")?.unwrap_or_default();
    let many = args.bool("many")?.unwrap_or(false);
    let reply = run(
        pool,
        doc! {
            "delete": &collection,
            "deletes": [{"q": filter, "limit": if many { 0 } else { 1 }}],
            "$db": &database,
        },
        None,
    )?;
    Ok(write_body(&reply))
}

fn write_body(reply: &Document) -> Value {
    let mut body = Map::new();
    body.insert("n".into(), number_field(reply, "n"));
    if reply.contains_key("nModified") {
        body.insert("nModified".into(), number_field(reply, "nModified"));
    }
    if let Some(upserted) = reply.get("upserted") {
        body.insert("upserted".into(), bson_to_json(upserted));
    }
    Value::Object(body)
}

fn documents_json(reply: &Document) -> Result<Value, String> {
    let documents = reply
        .get_array("documents")
        .map_err(|error| error.to_string())?;
    Ok(bson_to_json(&Bson::Array(documents.clone())))
}

fn number_field(doc: &Document, key: &str) -> Value {
    doc.get(key).map(bson_to_json).unwrap_or(json!(0))
}

/// Run a command and, when it returns a cursor, follow getMore until it is exhausted.
pub(crate) fn run(
    pool: &PgPool,
    command: Document,
    more: Option<(&str, &str)>,
) -> Result<Document, String> {
    let cursors = Mutex::new(Cursors::default());
    let mut reply = execute_command(pool, &cursors, command).map_err(explain)?;
    ensure_ok(&reply)?;
    if reply.get_document("cursor").is_err() {
        return Ok(reply);
    }
    let (db, collection) =
        more.ok_or_else(|| "cursor response is missing a namespace".to_owned())?;
    let mut documents = Vec::new();
    loop {
        let cursor = reply
            .get_document("cursor")
            .map_err(|error| error.to_string())?
            .clone();
        let batch_key = if cursor.contains_key("firstBatch") {
            "firstBatch"
        } else {
            "nextBatch"
        };
        documents.extend(
            cursor
                .get_array(batch_key)
                .map_err(|error| error.to_string())?
                .clone(),
        );
        let id = integer(
            cursor
                .get("id")
                .ok_or_else(|| "missing cursor id".to_owned())?,
        )
        .map_err(|error| error.to_string())?;
        if id == 0 {
            break;
        }
        reply = execute_command(
            pool,
            &cursors,
            doc! {
                "getMore": id,
                "collection": collection,
                "batchSize": MCP_BATCH,
                "$db": db,
            },
        )
        .map_err(explain)?;
        ensure_ok(&reply)?;
    }
    Ok(doc! {"documents": documents})
}

fn ensure_ok(doc: &Document) -> Result<(), String> {
    let ok = match doc.get("ok") {
        Some(Bson::Double(value)) => *value == 1.0,
        Some(Bson::Int32(value)) => *value == 1,
        Some(Bson::Int64(value)) => *value == 1,
        Some(Bson::Boolean(value)) => *value,
        None => true,
        Some(_) => false,
    };
    if !ok {
        return Err(doc.get_str("errmsg").unwrap_or("command failed").to_owned());
    }
    if doc.contains_key("writeErrors") {
        return Err(
            serde_json::to_string_pretty(&bson_to_json(&Bson::Document(doc.clone())))
                .unwrap_or_else(|_| "write failed".into()),
        );
    }
    Ok(())
}

fn explain(error: CommandExecutionError) -> String {
    format!("{} ({})", error.message, error.code_name)
}

struct Args<'a> {
    values: &'a Map<String, Value>,
}

impl<'a> Args<'a> {
    fn new(arguments: &'a Value, allowed: &[&str]) -> Result<Self, String> {
        let values = arguments
            .as_object()
            .ok_or_else(|| "arguments must be an object".to_owned())?;
        if let Some(key) = values.keys().find(|key| !allowed.contains(&key.as_str())) {
            return Err(format!("unexpected argument {key}"));
        }
        Ok(Self { values })
    }

    fn string(&self, key: &str) -> Result<String, String> {
        let value = self
            .values
            .get(key)
            .ok_or_else(|| format!("{key} is required"))?;
        let text = value
            .as_str()
            .ok_or_else(|| format!("{key} must be a string"))?;
        if text.is_empty() || text.len() > 63 || text.contains('\0') {
            return Err(format!("{key} must be 1..63 bytes and contain no NUL"));
        }
        Ok(text.to_owned())
    }

    fn document(&self, key: &str) -> Result<Option<Document>, String> {
        match self.values.get(key) {
            None => Ok(None),
            Some(Value::Object(_)) => match json_to_bson(self.values.get(key).unwrap(), 0)? {
                Bson::Document(document) => Ok(Some(document)),
                _ => Err(format!("{key} must be an object")),
            },
            Some(_) => Err(format!("{key} must be an object")),
        }
    }

    fn bool(&self, key: &str) -> Result<Option<bool>, String> {
        match self.values.get(key) {
            None => Ok(None),
            Some(Value::Bool(value)) => Ok(Some(*value)),
            Some(_) => Err(format!("{key} must be a boolean")),
        }
    }

    fn bounded_i64(&self, key: &str, min: i64, max: i64) -> Result<Option<i64>, String> {
        let Some(value) = self.values.get(key) else {
            return Ok(None);
        };
        let number = value
            .as_i64()
            .ok_or_else(|| format!("{key} must be an integer"))?;
        if number < min || number > max {
            return Err(format!("{key} must be between {min} and {max}"));
        }
        Ok(Some(number))
    }

    fn documents(&self) -> Result<Vec<Bson>, String> {
        let values = self
            .values
            .get("documents")
            .and_then(Value::as_array)
            .ok_or_else(|| "documents must be an array".to_owned())?;
        if values.is_empty() {
            return Err("documents cannot be empty".into());
        }
        if values.len() > 10_000 {
            return Err("write batch exceeds 10000 entries".into());
        }
        values
            .iter()
            .map(|value| match json_to_bson(value, 0)? {
                Bson::Document(document) => Ok(Bson::Document(document)),
                _ => Err("documents must contain objects".into()),
            })
            .collect()
    }

    fn pipeline(&self) -> Result<Vec<Bson>, String> {
        let values = self
            .values
            .get("pipeline")
            .and_then(Value::as_array)
            .ok_or_else(|| "pipeline must be an array".to_owned())?;
        if values.len() > 256 {
            return Err("pipeline exceeds 256 stages".into());
        }
        values
            .iter()
            .map(|value| match json_to_bson(value, 0)? {
                Bson::Document(document) => Ok(Bson::Document(document)),
                _ => Err("pipeline stages must be objects".into()),
            })
            .collect()
    }
}

fn json_to_bson(value: &Value, depth: usize) -> Result<Bson, String> {
    if depth > 64 {
        return Err("document exceeds nesting limit".into());
    }
    match value {
        Value::Null => Ok(Bson::Null),
        Value::Bool(value) => Ok(Bson::Boolean(*value)),
        Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                if i32::try_from(value).is_ok() {
                    Ok(Bson::Int32(value as i32))
                } else {
                    Ok(Bson::Int64(value))
                }
            } else if let Some(value) = number.as_u64() {
                i64::try_from(value)
                    .map(Bson::Int64)
                    .map_err(|_| "integer out of range".to_owned())
            } else {
                Ok(Bson::Double(number.as_f64().unwrap_or(0.0)))
            }
        }
        Value::String(value) => Ok(Bson::String(value.clone())),
        Value::Array(values) => Ok(Bson::Array(
            values
                .iter()
                .map(|value| json_to_bson(value, depth + 1))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        Value::Object(fields) => extended_or_document(fields, depth),
    }
}

fn extended_or_document(fields: &Map<String, Value>, depth: usize) -> Result<Bson, String> {
    if fields.len() == 1 {
        let (key, value) = fields.iter().next().unwrap();
        if let Some(bson) = extended_json(key, value)? {
            return Ok(bson);
        }
    }
    let mut document = Document::new();
    for (key, value) in fields {
        document.insert(key, json_to_bson(value, depth + 1)?);
    }
    Ok(Bson::Document(document))
}

fn extended_json(key: &str, value: &Value) -> Result<Option<Bson>, String> {
    match key {
        "$oid" => {
            let text = value
                .as_str()
                .ok_or_else(|| "$oid must be a string".to_owned())?;
            let id = bson::oid::ObjectId::parse_str(text).map_err(|_| "invalid $oid".to_owned())?;
            Ok(Some(Bson::ObjectId(id)))
        }
        "$numberInt" => {
            let number = i32::try_from(extended_int(value, "$numberInt")?)
                .map_err(|_| "invalid $numberInt".to_owned())?;
            Ok(Some(Bson::Int32(number)))
        }
        "$numberLong" => Ok(Some(Bson::Int64(extended_int(value, "$numberLong")?))),
        "$numberDouble" => {
            let text = value
                .as_str()
                .ok_or_else(|| "$numberDouble must be a string".to_owned())?;
            let number = text
                .parse::<f64>()
                .map_err(|_| "invalid $numberDouble".to_owned())?;
            Ok(Some(Bson::Double(number)))
        }
        "$date" => match value {
            Value::String(text) => {
                let date = bson::DateTime::parse_rfc3339_str(text)
                    .map_err(|_| "invalid $date".to_owned())?;
                Ok(Some(Bson::DateTime(date)))
            }
            Value::Object(fields) => {
                let millis = extended_int(
                    fields
                        .get("$numberLong")
                        .ok_or_else(|| "invalid $date".to_owned())?,
                    "$date",
                )?;
                Ok(Some(Bson::DateTime(bson::DateTime::from_millis(millis))))
            }
            _ => Err("invalid $date".into()),
        },
        _ => Ok(None),
    }
}

fn extended_int(value: &Value, key: &str) -> Result<i64, String> {
    value
        .as_str()
        .ok_or_else(|| format!("{key} must be a string"))?
        .parse::<i64>()
        .map_err(|_| format!("invalid {key}"))
}

fn bson_to_json(value: &Bson) -> Value {
    match value {
        Bson::Double(number) => serde_json::Number::from_f64(*number)
            .map(Value::Number)
            .unwrap_or_else(|| json!({"$numberDouble": number.to_string()})),
        Bson::String(value) => Value::String(value.clone()),
        Bson::Array(values) => Value::Array(values.iter().map(bson_to_json).collect()),
        Bson::Document(document) => {
            let mut fields = Map::new();
            for (key, value) in document {
                fields.insert(key.clone(), bson_to_json(value));
            }
            Value::Object(fields)
        }
        Bson::Boolean(value) => Value::Bool(*value),
        Bson::Null => Value::Null,
        Bson::Int32(value) => json!(value),
        Bson::Int64(value) => i64_to_json(*value),
        Bson::ObjectId(id) => json!({"$oid": id.to_hex()}),
        Bson::DateTime(date) => {
            json!({"$date": {"$numberLong": date.timestamp_millis().to_string()}})
        }
        Bson::Decimal128(number) => json!({"$numberDecimal": number.to_string()}),
        other => json!({"$bson": format!("{other:?}")}),
    }
}

fn i64_to_json(value: i64) -> Value {
    const MAX_SAFE: i64 = 1 << 53;
    if (-MAX_SAFE..=MAX_SAFE).contains(&value) {
        json!(value)
    } else {
        json!({"$numberLong": value.to_string()})
    }
}
