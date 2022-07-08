#![allow(dead_code)]
use crate::commands::{
    BuildInfo, Find, Handler, Insert, IsMaster, ListCollections, ListDatabases, Ping,
};
use crate::wire::{OpCode, UnknownCommandError};
use bson::{doc, Bson, Document};

#[derive(Debug, Clone)]
pub struct Request<'a> {
    id: u32,
    op_code: &'a OpCode,
    docs: Vec<Document>,
}

impl<'a> Request<'a> {
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

pub fn handle(id: u32, op_code: OpCode) -> Result<Vec<u8>, UnknownCommandError> {
    match route(&op_code) {
        Ok(doc) => {
            println!("Response: {:?}", doc);
            let request = Request {
                id,
                op_code: &op_code,
                docs: vec![doc],
            };
            Ok(op_code.reply(request).unwrap())
        }
        Err(e) => Err(e),
    }
}

fn run(docs: &Vec<Document>) -> Result<Document, UnknownCommandError> {
    let command = docs[0].keys().next().unwrap();

    println!("******\n*** OP_MSG Command: {}\n******\n", command);

    if command == "isMaster" || command == "ismaster" {
        IsMaster::new().handle(docs)
    } else if command == "buildInfo" || command == "buildinfo" {
        BuildInfo::new().handle(docs)
    } else if command == "listDatabases" {
        ListDatabases::new().handle(docs)
    } else if command == "listCollections" {
        ListCollections::new().handle(docs)
    } else if command == "find" {
        Find::new().handle(docs)
    } else if command == "ping" {
        Ping::new().handle(docs)
    } else if command == "insert" {
        Insert::new().handle(docs)
    } else {
        println!("Got unknown OP_MSG command: {}", command);
        Ok(doc! {
            "ok": Bson::Double(0.0),
            "errmsg": Bson::String("Unknown OP_MSG command".to_string()),
            "code": Bson::Int32(59),
            "codeName": "CommandNotFound",
        })
        // Err(UnknownCommandError::new(command.to_string()))
    }
}

fn run_op_query(docs: &Vec<Document>) -> Result<Document, UnknownCommandError> {
    let command = docs[0].keys().next().unwrap();

    println!("******\n*** OP_QUERY Command: {}\n******\n", command);

    if command == "isMaster" || command == "ismaster" {
        IsMaster::new().handle(docs)
    } else {
        println!("Got unknown OP_QUERY command: {}", command);
        Ok(doc! {
            "ok": Bson::Double(0.0),
            "errmsg": Bson::String("Unknown OP_QUERY command".to_string()),
            "code": Bson::Int32(59),
            "codeName": "CommandNotFound",
        })
    }
}

fn route(msg: &OpCode) -> Result<Document, UnknownCommandError> {
    match msg {
        OpCode::OpMsg(msg) => run(&msg.sections[0].documents),
        OpCode::OpQuery(query) => run_op_query(&vec![query.query.clone()]),
        _ => {
            println!("*** Got unknown opcode: {:?}", msg);
            Err(UnknownCommandError::new("unknown".to_string()))
        }
    }
}
