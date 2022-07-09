use crate::pg::PgDb;
use crate::{commands::Handler, handler::CommandExecutionError};
use bson::{bson, doc, Bson, Document};

pub struct DbStats {}

impl Handler for DbStats {
    fn new() -> Self {
        DbStats {}
    }

    fn handle(&self, docs: &Vec<Document>) -> Result<Document, CommandExecutionError> {}
}
