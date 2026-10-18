use crate::{
    commands::{check_options, Handler},
    handler::{CommandExecutionError, Request},
    parser::parse_update,
    pg::{optional_bool, SqlParam},
};
use bson::{doc, Document};
pub struct Update;
impl Handler for Update {
    fn new() -> Self {
        Self
    }
    fn handle(
        &self,
        request: &Request,
        docs: &[Document],
    ) -> Result<Document, CommandExecutionError> {
        let doc = &docs[0];
        check_options(doc, &["update", "updates", "ordered"])?;
        let sp = SqlParam::from(doc, "update")?;
        let updates = doc.get_array("updates")?;
        if updates.is_empty() {
            return Err(CommandExecutionError::new("updates cannot be empty".into()));
        }
        if doc.get_array("updates")?.len() > 10_000 {
            return Err(CommandExecutionError::new(
                "write batch exceeds 10000 entries".into(),
            ));
        }
        let ordered = optional_bool(doc, "ordered", true)?;
        let mut client = request.get_client()?;
        let (mut n, mut modified) = (0_i64, 0_i64);
        let mut upserted = Vec::new();
        let mut errors = Vec::new();
        for (index, value) in updates.iter().enumerate() {
            let result = (|| -> Result<_, CommandExecutionError> {
                let update = value.as_document().ok_or_else(|| {
                    CommandExecutionError::new("update entry must be a document".into())
                })?;
                check_options(update, &["q", "u", "upsert", "multi"])?;
                Ok(client.update(
                    &sp,
                    Some(update.get_document("q")?),
                    None,
                    parse_update(update.get_document("u")?)?,
                    optional_bool(update, "upsert", false)?,
                    optional_bool(update, "multi", false)?,
                )?)
            })();
            match result {
                Ok(result) => {
                    n += result.matched as i64;
                    modified += result.modified as i64;
                    if let Some(id) = result.upserted {
                        n += 1;
                        upserted.push(doc! {"index":index as i32,"_id":id});
                    }
                }
                Err(error) => {
                    errors
                        .push(doc! {"index":index as i32,"code":error.code,"errmsg":error.message});
                    if ordered {
                        break;
                    }
                }
            }
        }
        let mut reply = doc! {"ok":1.0,"n":n,"nModified":modified};
        if !upserted.is_empty() {
            reply.insert("upserted", upserted);
        }
        if !errors.is_empty() {
            reply.insert("writeErrors", errors);
        }
        Ok(reply)
    }
}
