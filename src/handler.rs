#![allow(dead_code)]
use crate::commands::{
    Aggregate, BuildInfo, CollStats, ConnectionStatus, Count, Create, CreateIndexes, DbStats,
    Delete, Drop, DropDatabase, Find, FindAndModify, GetCmdLineOpts, GetParameter, Handler, Hello,
    Insert, IsMaster, ListCollections, ListDatabases, ListIndexes, Ping, Update, WhatsMyUri,
};
use crate::cursor::Cursors;
use crate::pg::PgDb;
use crate::wire::{OpCode, OpMsg};
use bson::{doc, Bson, Document};
use postgres::NoTls;
use r2d2_postgres::PostgresConnectionManager;
use std::net::SocketAddr;
use std::sync::Mutex;

pub struct Request<'a> {
    pub cursors: &'a Mutex<Cursors>,
    pool: &'a r2d2::Pool<PostgresConnectionManager<NoTls>>,
    peer_addr: SocketAddr,
    op_code: &'a OpCode,
}

impl<'a> Request<'a> {
    pub fn new(
        pool: &'a r2d2::Pool<PostgresConnectionManager<NoTls>>,
        peer_addr: SocketAddr,
        op_code: &'a OpCode,
        cursors: &'a Mutex<Cursors>,
    ) -> Self {
        Request {
            cursors,
            pool,
            peer_addr,
            op_code,
        }
    }

    pub fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    pub fn get_op_code(&self) -> &OpCode {
        self.op_code
    }

    pub fn get_client(&self) -> Result<PgDb, CommandExecutionError> {
        Ok(PgDb::try_from_pool(self.pool)?)
    }
}

#[derive(Debug, Clone)]
pub struct Response<'a> {
    id: u32,
    op_code: &'a OpCode,
    docs: Vec<Document>,
}

impl<'a> Response<'a> {
    pub fn new(id: u32, op_code: &'a OpCode, docs: Vec<Document>) -> Self {
        Response { id, op_code, docs }
    }

    pub fn get_doc(&self) -> &Document {
        &self.docs[0]
    }

    pub fn get_id(&self) -> u32 {
        self.id
    }

    pub fn get_op_code(&self) -> &OpCode {
        self.op_code
    }
}

#[derive(Debug, Clone)]
pub struct CommandExecutionError {
    pub message: String,
    pub code: i32,
    pub code_name: String,
}

impl std::error::Error for CommandExecutionError {}

impl std::fmt::Display for CommandExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl CommandExecutionError {
    pub fn new(message: String) -> Self {
        CommandExecutionError {
            message,
            code: 2,
            code_name: "BadValue".into(),
        }
    }
}

impl From<eyre::Report> for CommandExecutionError {
    fn from(error: eyre::Report) -> Self {
        if let Some(error) = error.downcast_ref::<postgres::Error>() {
            if error.code() == Some(&postgres::error::SqlState::UNIQUE_VIOLATION) {
                return Self {
                    message: "duplicate key".into(),
                    code: 11000,
                    code_name: "DuplicateKey".into(),
                };
            }
            if let Some(db) = error.as_db_error() {
                return Self::new(db.message().to_owned());
            }
        }
        Self::new(error.to_string())
    }
}
impl From<bson::document::ValueAccessError> for CommandExecutionError {
    fn from(error: bson::document::ValueAccessError) -> Self {
        Self::new(error.to_string())
    }
}
impl From<crate::parser::InvalidUpdateError> for CommandExecutionError {
    fn from(error: crate::parser::InvalidUpdateError) -> Self {
        Self::new(error.to_string())
    }
}

pub fn handle(
    id: u32,
    pool: &r2d2::Pool<PostgresConnectionManager<NoTls>>,
    peer_addr: SocketAddr,
    op_code: &OpCode,
) -> Result<Vec<u8>, CommandExecutionError> {
    handle_with_cursors(
        id,
        pool,
        peer_addr,
        op_code,
        &Mutex::new(Cursors::default()),
    )
}

pub fn handle_with_cursors(
    id: u32,
    pool: &r2d2::Pool<PostgresConnectionManager<NoTls>>,
    peer_addr: SocketAddr,
    op_code: &OpCode,
    cursors: &Mutex<Cursors>,
) -> Result<Vec<u8>, CommandExecutionError> {
    let request = Request {
        cursors,
        pool,
        peer_addr,
        op_code,
    };
    match route(&request) {
        Ok(doc) => {
            log::trace!("Sending response: {:#?}", doc);
            let response = Response {
                id,
                op_code,
                docs: vec![doc],
            };
            op_code.reply(response).map_err(CommandExecutionError::from)
        }
        Err(e) => Err(e),
    }
}

fn run(request: &Request, docs: &[Document]) -> Result<Document, CommandExecutionError> {
    let command = docs
        .first()
        .and_then(|doc| doc.keys().next())
        .ok_or_else(|| CommandExecutionError::new("empty command".into()))?;

    log::debug!("OP_MSG command: {}", command);
    log::trace!("Received document: {:#?}", docs);

    if command == "getMore" || command == "killCursors" {
        let doc = &docs[0];
        let db = doc.get_str("$db")?;
        let collection = doc.get_str(if command == "getMore" {
            "collection"
        } else {
            "killCursors"
        })?;
        let ns = format!("{db}.{collection}");
        let mut cursors = request
            .cursors
            .lock()
            .map_err(|_| CommandExecutionError::new("cursor lock unavailable".into()))?;
        if command == "getMore" {
            crate::commands::check_options(doc, &["getMore", "collection", "batchSize"])?;
            let id = crate::pg::integer(
                doc.get("getMore")
                    .ok_or_else(|| CommandExecutionError::new("missing getMore".into()))?,
            )?;
            let batch = match doc.get("batchSize") {
                Some(value) => crate::pg::integer(value)?,
                None => 101,
            };
            if batch <= 0 {
                return Err(CommandExecutionError::new(
                    "getMore batchSize must be positive".into(),
                ));
            }
            return cursors
                .more(id, &ns, (batch as usize).min(crate::pg::MAX_QUERY_ROWS))
                .map_err(|e| CommandExecutionError {
                    message: e.to_string(),
                    code: 43,
                    code_name: "CursorNotFound".into(),
                });
        }
        crate::commands::check_options(doc, &["killCursors", "cursors"])?;
        let mut killed = Vec::new();
        let mut missing = Vec::new();
        for value in doc.get_array("cursors")? {
            let id = crate::pg::integer(value)?;
            if cursors.kill(id, &ns) {
                killed.push(id);
            } else {
                missing.push(id);
            }
        }
        Ok(
            doc! {"ok":1.0,"cursorsKilled":killed,"cursorsNotFound":missing,"cursorsAlive":[],"cursorsUnknown":[]},
        )
    } else if command == "find" {
        Find::new().handle(request, docs)
    } else if command == "findAndModify" {
        FindAndModify::new().handle(request, docs)
    } else if command == "count" {
        Count::new().handle(request, docs)
    } else if command == "aggregate" {
        Aggregate::new().handle(request, docs)
    } else if command == "insert" {
        Insert::new().handle(request, docs)
    } else if command == "update" {
        Update::new().handle(request, docs)
    } else if command == "delete" {
        Delete::new().handle(request, docs)
    } else if command == "create" {
        Create::new().handle(request, docs)
    } else if command == "createIndexes" {
        CreateIndexes::new().handle(request, docs)
    } else if command == "drop" {
        Drop::new().handle(request, docs)
    } else if command == "dropDatabase" {
        DropDatabase::new().handle(request, docs)
    } else if command == "isMaster" || command == "ismaster" {
        IsMaster::new().handle(request, docs)
    } else if command == "buildInfo" || command == "buildinfo" {
        BuildInfo::new().handle(request, docs)
    } else if command == "whatsmyuri" {
        WhatsMyUri::new().handle(request, docs)
    } else if command == "dbStats" {
        DbStats::new().handle(request, docs)
    } else if command == "collStats" {
        CollStats::new().handle(request, docs)
    } else if command == "listDatabases" {
        ListDatabases::new().handle(request, docs)
    } else if command == "listCollections" {
        ListCollections::new().handle(request, docs)
    } else if command == "listIndexes" {
        ListIndexes::new().handle(request, docs)
    } else if command == "ping" {
        Ping::new().handle(request, docs)
    } else if command == "hello" {
        Hello::new().handle(request, docs)
    } else if command == "getCmdLineOpts" {
        GetCmdLineOpts::new().handle(request, docs)
    } else if command == "getParameter" {
        GetParameter::new().handle(request, docs)
    } else if command == "connectionStatus" {
        ConnectionStatus::new().handle(request, docs)
    } else {
        log::error!("Got unknown OP_MSG command: {}\n{:?}", command, docs);
        Ok(doc! {
            "ok": Bson::Double(0.0),
            "errmsg": Bson::String(format!("no such command: '{}'", command).to_string()),
            "code": Bson::Int32(59),
            "codeName": "CommandNotFound",
        })
    }
}

fn run_op_query(request: &Request, docs: &[Document]) -> Result<Document, CommandExecutionError> {
    let empty = "".to_string();
    let command = docs[0].keys().next().unwrap_or(&empty);

    log::debug!("OP_QUERY Command: {}", command);

    if command.is_empty() || command == "isMaster" || command == "ismaster" {
        IsMaster::new().handle(request, docs)
    } else {
        log::error!("Got unknown OP_QUERY command: {}", command);
        Ok(doc! {
            "ok": Bson::Double(0.0),
            "errmsg": Bson::String(format!("no such command: '{}'", command).to_string()),
            "code": Bson::Int32(59),
            "codeName": "CommandNotFound",
        })
    }
}

fn handle_op_msg(request: &Request, msg: OpMsg) -> Result<Document, CommandExecutionError> {
    let bodies = msg
        .sections
        .iter()
        .filter(|s| s.kind == 0)
        .collect::<Vec<_>>();
    if bodies.len() != 1 || bodies[0].documents.len() != 1 {
        return Err(CommandExecutionError::new(
            "OP_MSG requires exactly one command body".into(),
        ));
    }
    let mut command = bodies[0].documents[0].clone();
    let mut names = std::collections::HashSet::new();
    for section in msg.sections.iter().filter(|s| s.kind == 1) {
        let identifier = section
            .identifier
            .as_deref()
            .unwrap_or("")
            .trim_end_matches('\0');
        if !["documents", "updates", "deletes"].contains(&identifier)
            || command.contains_key(identifier)
            || !names.insert(identifier)
        {
            return Err(CommandExecutionError::new(
                "invalid or duplicate document sequence".into(),
            ));
        }
        command.insert(identifier, section.documents.clone());
    }
    run(request, &[command])
}

fn route(request: &Request) -> Result<Document, CommandExecutionError> {
    match request.op_code {
        OpCode::OpMsg(msg) => handle_op_msg(request, msg.clone()),
        OpCode::OpQuery(query) => run_op_query(request, std::slice::from_ref(&query.query)),
        _ => {
            log::error!("Unroutable opcode received: {:?}", request.op_code);
            Err(CommandExecutionError::new(format!(
                "can't handle opcode: {:?}",
                request.op_code
            )))
        }
    }
}
