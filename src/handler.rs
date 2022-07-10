#![allow(dead_code)]
use crate::commands::{
    BuildInfo, CollStats, DbStats, Drop, Find, GetCmdLineOpts, Handler, Hello, Insert, IsMaster,
    ListCollections, ListDatabases, Ping, WhatsMyUri,
};
use crate::wire::OpCode;
use bson::{doc, Bson, Document};
use std::net::SocketAddr;

pub struct Request<'a> {
    peer_addr: SocketAddr,
    op_code: &'a OpCode,
}

impl<'a> Request<'a> {
    pub fn new(peer_addr: SocketAddr, op_code: &'a OpCode) -> Self {
        Request { peer_addr, op_code }
    }

    pub fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    pub fn get_op_code(&self) -> &OpCode {
        self.op_code
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
    peer_addr: SocketAddr,
    op_code: &OpCode,
) -> Result<Vec<u8>, CommandExecutionError> {
    let request = Request {
        peer_addr,
        op_code: &op_code,
    };
    match route(&request) {
        Ok(doc) => {
            log::debug!("Sending response: {:#?}", doc);
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
    log::debug!("Received document: {:#?}", docs);

    if command == "isMaster" || command == "ismaster" {
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
    } else if command == "find" {
        Find::new().handle(request, docs)
    } else if command == "ping" {
        Ping::new().handle(request, docs)
    } else if command == "hello" {
        Hello::new().handle(request, docs)
    } else if command == "getCmdLineOpts" {
        GetCmdLineOpts::new().handle(request, docs)
    } else if command == "insert" {
        Insert::new().handle(request, docs)
    } else if command == "drop" {
        Drop::new().handle(request, docs)
    } else {
        log::error!("Got unknown OP_MSG command: {}", command);
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
    let command = docs[0].keys().next().unwrap();

    log::debug!("OP_QUERY Command: {}", command);

    if command == "isMaster" || command == "ismaster" {
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

fn route(request: &Request) -> Result<Document, CommandExecutionError> {
    match request.op_code {
        OpCode::OpMsg(msg) => run(request, &msg.sections[0].documents),
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
