use crate::wire::UnknownCommandError;
use bson::Document;

mod build_info;
mod find;
mod insert;
mod is_master;
mod list_databases;

pub use self::build_info::BuildInfo;
pub use self::find::Find;
pub use self::insert::Insert;
pub use self::is_master::IsMaster;
pub use self::list_databases::ListDatabases;

pub trait Handler {
    fn new() -> Self;
    fn handle(&self, msg: &Vec<Document>) -> Result<Document, UnknownCommandError>;
}
