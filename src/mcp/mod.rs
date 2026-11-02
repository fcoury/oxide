//! Stdio [Model Context Protocol](https://modelcontextprotocol.io) server.
//!
//! The protocol surface is small and stable (initialize, tools/list, tools/call, ping),
//! so this is newline-delimited JSON-RPC rather than another SDK. Tool calls build
//! MongoDB command documents and run them through [`crate::handler::execute_command`].
//! Writes are omitted unless the process was started with `--allow-writes`.
mod tools;

use eyre::{Result, WrapErr};
use postgres::NoTls;
use r2d2_postgres::PostgresConnectionManager;
use serde_json::{json, Value};
use std::io::{BufRead, IsTerminal, Write};
use std::time::Duration;

const MAX_MESSAGE: usize = 8 * 1024 * 1024;
const SUPPORTED_VERSIONS: &[&str] = &["2024-11-05", "2025-03-26", "2025-06-18", "2025-11-25"];
const PROTOCOL_VERSION: &str = "2025-11-25";
const INSTRUCTIONS: &str = "\
Oxide serves a PostgreSQL-backed MongoDB database. \
list_databases, list_collections, find, aggregate, and count are read-only. \
find returns at most 20 documents unless limit is set; limit 0 uses the server cap of 10000. \
insert, update, and delete exist only when the server was started with --allow-writes, \
and a write call without that flag does not change data. \
Queries use Oxide's supported MongoDB subset.";

type PgPool = r2d2::Pool<PostgresConnectionManager<NoTls>>;

pub struct Server {
    pool: PgPool,
    allow_writes: bool,
}

impl Server {
    pub fn connect(postgres_url: &str, allow_writes: bool) -> Result<Self> {
        let manager = PostgresConnectionManager::new(
            postgres_url.parse().wrap_err("invalid PostgreSQL URL")?,
            NoTls,
        );
        let pool = r2d2::Pool::builder()
            .max_size(4)
            .connection_timeout(Duration::from_secs(5))
            .build(manager)
            .wrap_err("could not connect to PostgreSQL")?;
        // Inserts install the same change-capture trigger as the wire server.
        // Prepare the journal once per process so parallel clients do not race.
        static JOURNAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _guard = JOURNAL.lock().unwrap_or_else(|error| error.into_inner());
        let mut client = crate::pg::PgDb::try_from_pool(&pool)?;
        crate::change_stream::initialize(&mut client)
            .wrap_err("could not prepare the Oxide change journal")?;
        Ok(Self { pool, allow_writes })
    }

    #[cfg(test)]
    fn from_pool(pool: PgPool, allow_writes: bool) -> Self {
        Self { pool, allow_writes }
    }

    /// Read newline-delimited JSON-RPC from `reader` and write one JSON line per response.
    pub fn serve<R: BufRead, W: Write>(&self, mut reader: R, mut writer: W) -> Result<()> {
        let mut line = String::new();
        loop {
            line.clear();
            let read = reader.read_line(&mut line)?;
            if read == 0 {
                break;
            }
            if line.len() > MAX_MESSAGE {
                writeln!(
                    writer,
                    "{}",
                    rpc_error(None, -32700, "message exceeds 8 MiB")
                )?;
                writer.flush()?;
                continue;
            }
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            if let Some(response) = self.handle_message(trimmed) {
                writeln!(writer, "{response}")?;
                writer.flush()?;
            }
        }
        Ok(())
    }

    /// Handle one JSON-RPC message. Notifications return `None`.
    pub fn handle_message(&self, line: &str) -> Option<String> {
        let message: Value = match serde_json::from_str(line) {
            Ok(message) => message,
            Err(_) => return Some(rpc_error(None, -32700, "Parse error")),
        };
        let Some(fields) = message.as_object() else {
            return Some(rpc_error(None, -32600, "Invalid Request"));
        };
        if fields.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
            return Some(rpc_error(
                fields.get("id"),
                -32600,
                "jsonrpc must be \"2.0\"",
            ));
        }
        let Some(method) = fields.get("method").and_then(Value::as_str) else {
            return Some(rpc_error(fields.get("id"), -32600, "method is required"));
        };
        let id = fields.get("id")?;
        let params = fields.get("params").cloned().unwrap_or_else(|| json!({}));
        Some(match method {
            "initialize" => initialize(id, &params),
            "ping" => rpc_result(id, json!({})),
            "tools/list" => rpc_result(id, json!({"tools": tools::definitions(self.allow_writes)})),
            "tools/call" => self.tools_call(id, &params),
            _ => rpc_error(Some(id), -32601, format!("Method not found: {method}")),
        })
    }

    fn tools_call(&self, id: &Value, params: &Value) -> String {
        let Some(name) = params.get("name").and_then(Value::as_str) else {
            return rpc_error(Some(id), -32602, "tools/call requires a name");
        };
        let arguments = match params.get("arguments") {
            None | Some(Value::Null) => json!({}),
            Some(value @ Value::Object(_)) => value.clone(),
            Some(_) => return tool_result(id, "arguments must be an object", true),
        };
        if !known_tool(name) {
            return rpc_error(Some(id), -32602, format!("unknown tool: {name}"));
        }
        if tools::is_write(name) && !self.allow_writes {
            return tool_result(
                id,
                "writes are disabled; restart oxide mcp with --allow-writes to insert, update, or delete",
                true,
            );
        }
        log::debug!("mcp tool {name}");
        match tools::call(&self.pool, name, &arguments) {
            Ok(value) => match serde_json::to_string_pretty(&value) {
                Ok(text) if text.len() > MAX_MESSAGE / 2 => tool_result(
                    id,
                    "result exceeds 4 MiB; narrow the query with a filter or limit",
                    true,
                ),
                Ok(text) => tool_result(id, text, false),
                Err(error) => tool_result(id, error.to_string(), true),
            },
            Err(message) => tool_result(id, message, true),
        }
    }
}

fn known_tool(name: &str) -> bool {
    matches!(
        name,
        "list_databases"
            | "list_collections"
            | "find"
            | "aggregate"
            | "count"
            | "insert"
            | "update"
            | "delete"
    )
}

pub fn start(postgres_url: &str, allow_writes: bool) -> Result<()> {
    let server = Server::connect(postgres_url, allow_writes)?;
    if std::io::stdin().is_terminal() {
        log::warn!("oxide mcp speaks JSON-RPC on stdin/stdout; this log stays on stderr");
    }
    log::info!(
        "oxide mcp ready on stdio ({})",
        if allow_writes {
            "writes enabled"
        } else {
            "read-only"
        }
    );
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    server.serve(stdin.lock(), stdout.lock())
}

fn initialize(id: &Value, params: &Value) -> String {
    let requested = params
        .get("protocolVersion")
        .and_then(Value::as_str)
        .unwrap_or("");
    let version = if SUPPORTED_VERSIONS.contains(&requested) {
        requested
    } else {
        PROTOCOL_VERSION
    };
    rpc_result(
        id,
        json!({
            "protocolVersion": version,
            "capabilities": {"tools": {}},
            "serverInfo": {
                "name": "oxide",
                "title": "Oxide",
                "version": env!("CARGO_PKG_VERSION"),
                "description": "PostgreSQL-backed MongoDB database for MCP clients"
            },
            "instructions": INSTRUCTIONS
        }),
    )
}

fn rpc_result(id: &Value, result: Value) -> String {
    serde_json::to_string(&json!({"jsonrpc": "2.0", "id": id, "result": result}))
        .unwrap_or_else(|_| rpc_error(Some(id), -32603, "response encoding failed"))
}

fn rpc_error(id: Option<&Value>, code: i32, message: impl Into<String>) -> String {
    let id = id.cloned().unwrap_or(Value::Null);
    format!(
        r#"{{"jsonrpc":"2.0","id":{},"error":{{"code":{},"message":{}}}}}"#,
        serde_json::to_string(&id).unwrap_or_else(|_| "null".into()),
        code,
        serde_json::to_string(&message.into()).unwrap_or_else(|_| "\"error\"".into())
    )
}

fn tool_result(id: &Value, text: impl Into<String>, is_error: bool) -> String {
    rpc_result(
        id,
        json!({
            "content": [{"type": "text", "text": text.into()}],
            "isError": is_error
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::io::Cursor;

    fn idle(allow_writes: bool) -> Server {
        let manager = PostgresConnectionManager::new(
            "postgresql://127.0.0.1:1/unused".parse().unwrap(),
            NoTls,
        );
        let pool = r2d2::Pool::builder()
            .min_idle(Some(0))
            .max_size(1)
            .connection_timeout(Duration::from_secs(1))
            .build_unchecked(manager);
        Server::from_pool(pool, allow_writes)
    }

    fn request(method: &str, params: Value) -> Value {
        json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    }

    fn roundtrip(server: &Server, message: Value) -> Value {
        let raw = server
            .handle_message(&message.to_string())
            .expect("response");
        assert!(
            !raw.contains('\n'),
            "stdio messages must be one line: {raw}"
        );
        serde_json::from_str(&raw).unwrap()
    }

    #[test]
    fn initialize_negotiates_the_protocol_version() {
        let server = idle(false);
        let supported = roundtrip(
            &server,
            request(
                "initialize",
                json!({"protocolVersion": "2025-03-26", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}}),
            ),
        );
        assert_eq!(supported["result"]["protocolVersion"], "2025-03-26");
        assert_eq!(supported["result"]["serverInfo"]["name"], "oxide");
        assert!(supported["result"]["capabilities"]["tools"].is_object());

        let fallback = roundtrip(
            &server,
            request("initialize", json!({"protocolVersion": "1999-01-01"})),
        );
        assert_eq!(fallback["result"]["protocolVersion"], PROTOCOL_VERSION);
    }

    #[test]
    fn tools_list_hides_writes_until_they_are_enabled() {
        let read_only = roundtrip(&idle(false), request("tools/list", json!({})));
        let names: Vec<_> = read_only["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|tool| tool["name"].as_str().unwrap())
            .collect();
        assert_eq!(
            names,
            [
                "list_databases",
                "list_collections",
                "find",
                "aggregate",
                "count"
            ]
        );
        assert_eq!(
            read_only["result"]["tools"][2]["annotations"]["readOnlyHint"],
            true
        );

        let writable = roundtrip(&idle(true), request("tools/list", json!({})));
        let names: Vec<_> = writable["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|tool| tool["name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"insert"));
        let delete = writable["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .find(|tool| tool["name"] == "delete")
            .unwrap();
        assert_eq!(delete["annotations"]["readOnlyHint"], false);
        assert_eq!(delete["annotations"]["destructiveHint"], true);
    }

    #[test]
    fn write_calls_are_refused_without_the_flag() {
        let response = roundtrip(
            &idle(false),
            request(
                "tools/call",
                json!({"name": "insert", "arguments": {"database": "demo", "collection": "books", "documents": [{"title": "nope"}]}}),
            ),
        );
        assert_eq!(response["result"]["isError"], true);
        let text = response["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("--allow-writes"), "{text}");
    }

    #[test]
    fn protocol_errors_and_notifications() {
        let server = idle(false);
        assert!(server
            .handle_message(r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#)
            .is_none());
        let parsed: Value =
            serde_json::from_str(&server.handle_message("not json").unwrap()).unwrap();
        assert_eq!(parsed["error"]["code"], -32700);

        let missing = roundtrip(&server, request("resources/list", json!({})));
        assert_eq!(missing["error"]["code"], -32601);

        let ping = roundtrip(&server, request("ping", json!({})));
        assert_eq!(ping["result"], json!({}));

        let bad_find = roundtrip(
            &server,
            request("tools/call", json!({"name": "find", "arguments": {}})),
        );
        assert_eq!(bad_find["result"]["isError"], true);
        assert!(bad_find["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("database"));
    }

    #[test]
    fn stdio_session_replies_once_per_request() {
        let input = concat!(
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"t","version":"0"}}}"#,
            "\n\n",
            r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
            "\n",
            r#"{"jsonrpc":"2.0","id":"list","method":"tools/list"}"#,
            "\n",
        );
        let mut output = Vec::new();
        idle(false)
            .serve(Cursor::new(input.as_bytes()), &mut output)
            .unwrap();
        let lines: Vec<_> = std::str::from_utf8(&output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0]["id"], 1);
        assert_eq!(lines[1]["id"], "list");
        assert!(lines[1]["result"]["tools"].as_array().unwrap().len() >= 5);
    }

    fn postgres_server(allow_writes: bool) -> Server {
        Server::connect(
            &std::env::var("DATABASE_URL").expect("DATABASE_URL"),
            allow_writes,
        )
        .unwrap()
    }

    fn call(server: &Server, name: &str, arguments: Value) -> Value {
        roundtrip(
            server,
            request("tools/call", json!({"name": name, "arguments": arguments})),
        )
    }

    fn payload(response: &Value) -> Value {
        assert_eq!(response["result"]["isError"], false, "{response}");
        serde_json::from_str(response["result"]["content"][0]["text"].as_str().unwrap()).unwrap()
    }

    #[test]
    fn reads_seeded_documents_and_refuses_writes() {
        let db = format!("mcp{}", uuid::Uuid::new_v4().simple());
        let writable = postgres_server(true);
        let reader = postgres_server(false);
        let _guard = SchemaGuard(db.clone());
        let inserted = payload(&call(
            &writable,
            "insert",
            json!({
                "database": &db,
                "collection": "books",
                "documents": [
                    {"title": "The Analytical Engine", "author": "Ada Lovelace", "genre": "history", "year": 1843},
                    {"title": "The Elements", "author": "Euclid", "genre": "math", "year": -300},
                    {"title": "Notes on the Engine", "author": "Ada Lovelace", "genre": "history", "year": 1842}
                ]
            }),
        ));
        assert_eq!(inserted["n"], 3);

        let databases = payload(&call(&reader, "list_databases", json!({})));
        assert!(databases["databases"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["name"] == db));

        let collections = payload(&call(&reader, "list_collections", json!({"database": &db})));
        assert_eq!(collections["collections"], json!(["books"]));

        let found = payload(&call(
            &reader,
            "find",
            json!({"database": &db, "collection": "books", "filter": {"author": "Ada Lovelace"}, "sort": {"year": 1}}),
        ));
        let titles: Vec<_> = found["documents"]
            .as_array()
            .unwrap()
            .iter()
            .map(|doc| doc["title"].as_str().unwrap())
            .collect();
        assert_eq!(titles, ["Notes on the Engine", "The Analytical Engine"]);
        let oid = found["documents"][0]["_id"]["$oid"]
            .as_str()
            .unwrap()
            .to_owned();
        let by_id = payload(&call(
            &reader,
            "find",
            json!({"database": &db, "collection": "books", "filter": {"_id": {"$oid": oid}}}),
        ));
        assert_eq!(by_id["documents"][0]["title"], "Notes on the Engine");

        let counted = payload(&call(
            &reader,
            "count",
            json!({"database": &db, "collection": "books", "query": {"genre": "history"}}),
        ));
        assert_eq!(counted["n"], 2);

        let grouped = payload(&call(
            &reader,
            "aggregate",
            json!({
                "database": &db,
                "collection": "books",
                "pipeline": [{"$group": {"_id": "$genre", "n": {"$sum": 1}}}]
            }),
        ));
        let history = grouped["documents"]
            .as_array()
            .unwrap()
            .iter()
            .find(|doc| doc["_id"] == "history")
            .unwrap();
        assert_eq!(history["n"], 2);

        let refused = call(
            &reader,
            "insert",
            json!({"database": &db, "collection": "books", "documents": [{"title": "should not land"}]}),
        );
        assert_eq!(refused["result"]["isError"], true);
        let after = payload(&call(
            &reader,
            "count",
            json!({"database": &db, "collection": "books"}),
        ));
        assert_eq!(after["n"], 3);

        let updated = payload(&call(
            &writable,
            "update",
            json!({
                "database": &db,
                "collection": "books",
                "filter": {"title": "The Elements"},
                "update": {"$set": {"genre": "geometry"}},
                "multi": false
            }),
        ));
        assert_eq!(updated["n"], 1);
        assert_eq!(updated["nModified"], 1);

        let deleted = payload(&call(
            &writable,
            "delete",
            json!({
                "database": &db,
                "collection": "books",
                "filter": {"title": "The Elements"},
                "many": false
            }),
        ));
        assert_eq!(deleted["n"], 1);
        let remaining = payload(&call(
            &reader,
            "count",
            json!({"database": &db, "collection": "books"}),
        ));
        assert_eq!(remaining["n"], 2);
    }

    #[test]
    fn find_limit_defaults_to_twenty_and_cursors_are_drained() {
        let db = format!("mcp{}", uuid::Uuid::new_v4().simple());
        let server = postgres_server(true);
        let _guard = SchemaGuard(db.clone());
        let documents: Vec<_> = (0..25).map(|n| json!({"n": n})).collect();
        payload(&call(
            &server,
            "insert",
            json!({"database": &db, "collection": "pages", "documents": documents}),
        ));
        let page = payload(&call(
            &server,
            "find",
            json!({"database": &db, "collection": "pages"}),
        ));
        assert_eq!(page["documents"].as_array().unwrap().len(), 20);
        let all = payload(&call(
            &server,
            "find",
            json!({"database": &db, "collection": "pages", "limit": 0}),
        ));
        assert_eq!(all["documents"].as_array().unwrap().len(), 25);

        let drained = tools::run(
            &server.pool,
            bson::doc! {
                "find": "pages",
                "filter": {},
                "batchSize": 1,
                "$db": &db,
            },
            Some((db.as_str(), "pages")),
        )
        .unwrap();
        assert_eq!(drained.get_array("documents").unwrap().len(), 25);
    }

    struct SchemaGuard(String);
    impl Drop for SchemaGuard {
        fn drop(&mut self) {
            if let Ok(url) = std::env::var("DATABASE_URL") {
                if let Ok(mut pg) = crate::pg::PgDb::try_with_uri(&url) {
                    let _ = pg.drop_schema(&self.0);
                }
            }
        }
    }
}
