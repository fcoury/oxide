use crate::commands::{Find, Handler, Insert, IsMaster, ListDatabases};
use crate::wire::{OpCode, UnknownCommandError};
use bson::Document;

pub fn handle(request_id: u32, op_code: OpCode) -> Result<Vec<u8>, UnknownCommandError> {
    match route(&op_code) {
        Ok(doc) => Ok(op_code.reply(request_id, doc).unwrap()),
        Err(e) => Err(e),
    }
}

fn run(doc: Document) -> Result<Document, UnknownCommandError> {
    let command = doc.keys().next().unwrap();

    println!("******\n*** Command: {}\n******\n", command);

    if command == "isMaster" {
        IsMaster::new().handle(doc)
    } else if command == "listDatabases" {
        ListDatabases::new().handle(doc)
    } else if command == "find" {
        Find::new().handle(doc)
    } else if command == "insert" {
        Insert::new().handle(doc)
    } else {
        println!("Got unknown command: {}", command);
        Err(UnknownCommandError)
    }
}

fn route(msg: &OpCode) -> Result<Document, UnknownCommandError> {
    match msg {
        OpCode::OpMsg(msg) => {
            let doc = msg.sections[0].documents[0].clone();
            run(doc)
        }
        OpCode::OpQuery(query) => {
            println!("*** Query: {:?}", query);
            run(query.query.clone())
        }
        _ => {
            println!("*** Got unknown opcode: {:?}", msg);
            Err(UnknownCommandError)
        }
    }
}
