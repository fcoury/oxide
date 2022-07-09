use crate::handler::CommandExecutionError;
use bson::Document;

mod build_info;
mod find;
mod insert;
mod is_master;
mod list_collections;
mod list_databases;
mod ping;

pub use self::build_info::BuildInfo;
pub use self::find::Find;
pub use self::insert::Insert;
pub use self::is_master::IsMaster;
pub use self::list_collections::ListCollections;
pub use self::list_databases::ListDatabases;
pub use self::ping::Ping;

pub trait Handler {
    fn new() -> Self;
    fn handle(&self, msg: &Vec<Document>) -> Result<Document, CommandExecutionError>;
}
