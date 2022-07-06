use crate::wire::UnknownCommandError;
use bson::Document;

mod is_master;
mod list_databases;

pub use self::is_master::IsMaster;
pub use self::list_databases::ListDatabases;

pub trait Handler {
  fn new() -> Self;
  fn handle(&self, msg: Document) -> Result<Document, UnknownCommandError>;
}
