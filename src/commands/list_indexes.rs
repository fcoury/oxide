use crate::{
    commands::{cursor_batch_size, Handler},
    handler::{CommandExecutionError, Request},
    pg::SqlParam,
};
use bson::{doc, Document};
pub struct ListIndexes;
impl Handler for ListIndexes {
    fn new() -> Self {
        Self
    }
    fn handle(
        &self,
        request: &Request,
        docs: &[Document],
    ) -> Result<Document, CommandExecutionError> {
        crate::commands::check_options(&docs[0], &["listIndexes", "cursor"])?;
        let batch = cursor_batch_size(&docs[0])?;
        let sp = SqlParam::from(&docs[0], "listIndexes")?;
        let mut client = request.get_client()?;
        if !sp.exists(&mut client)? {
            return Err(CommandExecutionError {
                code: 26,
                code_name: "NamespaceNotFound".into(),
                message: "collection does not exist".into(),
            });
        }
        let mut indexes = Vec::new();
        for row in client.get_table_indexes(&sp.db, &sp.collection)? {
            let definition: Option<String> = row.get("definition");
            if let Some(definition) = definition {
                let value: serde_json::Value =
                    serde_json::from_str(&definition).map_err(eyre::Report::from)?;
                let name = value["name"]
                    .as_str()
                    .ok_or_else(|| CommandExecutionError::new("invalid index metadata".into()))?;
                let mut key = Document::new();
                for (field, direction) in value["key"].as_object().ok_or_else(|| {
                    CommandExecutionError::new("invalid index key metadata".into())
                })? {
                    key.insert(
                        field,
                        direction.as_i64().ok_or_else(|| {
                            CommandExecutionError::new("invalid index direction".into())
                        })? as i32,
                    );
                }
                let index = doc! {"v":2,"name":name,"key":key,"unique":value["unique"].as_bool().unwrap_or(false)};
                indexes.push(index);
            }
        }
        Ok(request
            .cursors
            .lock()
            .map_err(|_| CommandExecutionError::new("cursor lock unavailable".into()))?
            .first(
                format!("{}.$cmd.listIndexes.{}", sp.db, sp.collection),
                indexes,
                batch,
                false,
            )?)
    }
}
