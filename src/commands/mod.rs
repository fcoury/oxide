use crate::handler::{CommandExecutionError, Request};
use bson::Document;

mod aggregate;
mod build_info;
mod coll_stats;
mod connection_status;
mod count;
mod create;
mod create_indexes;
mod db_stats;
mod delete;
mod drop;
mod drop_database;
mod find;
mod find_and_modify;
mod get_cmd_line_opts;
mod get_parameter;
mod hello;
mod insert;
mod is_master;
mod list_collections;
mod list_databases;
mod list_indexes;
mod ping;
mod update;
mod whats_my_uri;

pub use self::aggregate::Aggregate;
pub use self::aggregate::{compile_sql, compile_stream_pipeline};
pub use self::build_info::BuildInfo;
pub use self::coll_stats::CollStats;
pub use self::connection_status::ConnectionStatus;
pub use self::count::Count;
pub use self::create::Create;
pub use self::create_indexes::CreateIndexes;
pub use self::db_stats::DbStats;
pub use self::delete::Delete;
pub use self::drop::Drop;
pub use self::drop_database::DropDatabase;
pub use self::find::Find;
pub use self::find_and_modify::FindAndModify;
pub use self::get_cmd_line_opts::GetCmdLineOpts;
pub use self::get_parameter::GetParameter;
pub use self::hello::Hello;
pub use self::insert::Insert;
pub use self::is_master::IsMaster;
pub use self::list_collections::ListCollections;
pub use self::list_databases::ListDatabases;
pub use self::list_indexes::ListIndexes;
pub use self::ping::Ping;
pub use self::update::Update;
pub use self::whats_my_uri::WhatsMyUri;

pub trait Handler {
    fn new() -> Self;
    fn handle(
        &self,
        request: &Request,
        msg: &[Document],
    ) -> Result<Document, CommandExecutionError>;
}

/// Parse the cursor options shared by aggregation and catalog commands.
pub fn cursor_batch_size(doc: &Document) -> Result<usize, CommandExecutionError> {
    let size = match doc.get("cursor") {
        None => 101,
        Some(bson::Bson::Document(cursor)) => {
            if cursor.keys().any(|key| key != "batchSize") {
                return Err(CommandExecutionError::new(
                    "unsupported cursor option".into(),
                ));
            }
            cursor
                .get("batchSize")
                .map(crate::pg::integer)
                .transpose()?
                .unwrap_or(101)
        }
        _ => {
            return Err(CommandExecutionError::new(
                "cursor must be a document".into(),
            ))
        }
    };
    if size < 0 {
        return Err(CommandExecutionError::new(
            "batchSize must be nonnegative".into(),
        ));
    }
    Ok(size.min(crate::pg::MAX_QUERY_ROWS as i64) as usize)
}

/// Reject options for which this server cannot honor the requested semantics.
pub fn check_options(doc: &Document, allowed: &[&str]) -> Result<(), CommandExecutionError> {
    for key in doc.keys() {
        if key == "$db" || key == "comment" || allowed.contains(&key.as_str()) {
            continue;
        }
        if key == "$readPreference" {
            let preference = doc.get_document(key)?;
            if preference.len() == 1
                && [
                    "primary",
                    "primaryPreferred",
                    "secondary",
                    "secondaryPreferred",
                    "nearest",
                ]
                .contains(&preference.get_str("mode")?)
            {
                continue; // A standalone server has no replica selection to perform.
            }
        }
        if key == "writeConcern" {
            let concern = doc.get_document(key)?;
            if concern
                .iter()
                .all(|(k, v)| k == "w" && crate::pg::integer(v).is_ok_and(|n| n == 1))
            {
                continue;
            }
        }
        return Err(CommandExecutionError::new(format!(
            "unsupported option {key}"
        )));
    }
    Ok(())
}
