#![allow(dead_code)]
use crate::commands::{
    Aggregate, BuildInfo, CollStats, ConnectionStatus, Create, CreateIndexes, DbStats, Delete,
    Drop, DropDatabase, Find, FindAndModify, GetCmdLineOpts, GetParameter, Handler, Hello, Insert,
    IsMaster, ListCollections, ListDatabases, ListIndexes, Ping, Update, WhatsMyUri,
};
use crate::pg::PgDb;
use crate::wire::{OpCode, OpMsg};
use bson::{doc, Bson, Document};
use postgres::NoTls;
use r2d2_postgres::PostgresConnectionManager;
use std::net::SocketAddr;

pub struct Request<'a> {
    pool: &'a r2d2::Pool<PostgresConnectionManager<NoTls>>,
    peer_addr: SocketAddr,
    op_code: &'a OpCode,
}

impl<'a> Request<'a> {
    pub fn new(
        pool: &'a r2d2::Pool<PostgresConnectionManager<NoTls>>,
        peer_addr: SocketAddr,
        op_code: &'a OpCode,
    ) -> Self {
        Request {
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

    pub fn get_client(&self) -> PgDb {
        PgDb::new_from_pool(self.pool.clone())
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
}

impl std::error::Error for CommandExecutionError {}

impl std::fmt::Display for CommandExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl CommandExecutionError {
    pub fn new(message: String) -> Self {
        CommandExecutionError { message }
    }
}

pub fn handle(
    id: u32,
    pool: &r2d2::Pool<PostgresConnectionManager<NoTls>>,
    peer_addr: SocketAddr,
    op_code: &OpCode,
) -> Result<Vec<u8>, CommandExecutionError> {
    let request = Request {
        pool,
        peer_addr,
        op_code: &op_code,
    };
    match route(&request) {
        Ok(doc) => {
            log::trace!("Sending response: {:#?}", doc);
            let response = Response {
                id,
                op_code: &op_code,
                docs: vec![doc],
            };
            Ok(op_code.reply(response).unwrap())
        }
        Err(e) => Err(e),
    }
}

fn run(request: &Request, docs: &Vec<Document>) -> Result<Document, CommandExecutionError> {
    let command = docs[0].keys().next().unwrap();

    log::debug!("OP_MSG command: {}", command);
    log::trace!("Received document: {:#?}", docs);

    if command == "find" {
        Find::new().handle(request, docs)
    } else if command == "findAndModify" {
        FindAndModify::new().handle(request, docs)
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

fn run_op_query(
    request: &Request,
    docs: &Vec<Document>,
) -> Result<Document, CommandExecutionError> {
    let empty = "".to_string();
    let command = docs[0].keys().next().unwrap_or(&empty);

    log::debug!("OP_QUERY Command: {}", command);

    if command == "" || command == "isMaster" || command == "ismaster" {
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
    if msg.sections.len() < 1 {
        log::error!("Received OP_MSG with no sections:\n{:#?}", msg);
        return Err(CommandExecutionError::new(
            "OP_MSG must have at least one section, received none".to_string(),
        ));
    }

    let section = msg.sections[0].clone();
    if section.kind == 0 {
        return run(request, &section.documents);
    }

    if section.kind == 1 {
        if section.identifier.is_none() {
            log::error!(
                "Received a kind 1 section from OP_MSG with no identifier:\n{:#?}",
                msg
            );
            return Err(CommandExecutionError::new(
                "all kind 1 sections on OP_MSG must have an identifier, received none".to_string(),
            ));
        }

        let mut identifier = section.identifier.unwrap();
        identifier.pop();

        if identifier == "documents" {
            if msg.sections.len() < 2 {
                log::error!(
                    "Received a document kind 1 section with no matching kind 0:\n{:#?}",
                    msg
                );
                return Err(CommandExecutionError::new(
                    "OP_MSG with a kind 1 documents section must also have at least one kind 0 section, received none".to_string(),
                ));
            }

            let mut doc = msg.sections[1].documents[0].clone();
            doc.insert(identifier, section.documents.clone());
            return run(request, &vec![doc]);
        }

        log::error!(
            "Received unknown kind 1 section identifier from OP_MSG:\n{:#?}",
            msg
        );
        return Err(CommandExecutionError::new(
            format!(
                "received unknown kind 1 section identifier from OP_MSG: {}",
                identifier
            )
            .to_string(),
        ));
    }

    log::error!(
        "Received unknown section from OP_MSG: {}\n{:#?}",
        section.kind,
        msg
    );
    Err(CommandExecutionError::new(
        format!(
            "received unknown section kind from OP_MSG: {}",
            section.kind
        )
        .to_string(),
    ))
}

fn route(request: &Request) -> Result<Document, CommandExecutionError> {
    match request.op_code {
        OpCode::OpMsg(msg) => handle_op_msg(request, msg.clone()),
        OpCode::OpQuery(query) => run_op_query(request, &vec![query.query.clone()]),
        _ => {
            log::error!("Unroutable opcode received: {:?}", request.op_code);
            Err(CommandExecutionError::new(format!(
                "can't handle opcode: {:?}",
                request.op_code
            )))
        }
    }
}
