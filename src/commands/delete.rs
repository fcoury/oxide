use crate::{
    commands::{check_options, Handler},
    handler::{CommandExecutionError, Request},
    pg::{integer, optional_bool, SqlParam},
};
use bson::{doc, Document};
pub struct Delete;
impl Handler for Delete {
    fn new() -> Self {
        Self
    }
    fn handle(
        &self,
        request: &Request,
        docs: &[Document],
    ) -> Result<Document, CommandExecutionError> {
        let doc = &docs[0];
        check_options(doc, &["delete", "deletes", "ordered"])?;
        let sp = SqlParam::from(doc, "delete")?;
        let deletes = doc.get_array("deletes")?;
        if deletes.is_empty() {
            return Err(CommandExecutionError::new("deletes cannot be empty".into()));
        }
        if doc.get_array("deletes")?.len() > 10_000 {
            return Err(CommandExecutionError::new(
                "write batch exceeds 10000 entries".into(),
            ));
        }
        let ordered = optional_bool(doc, "ordered", true)?;
        let mut client = request.get_client()?;
        let exists = sp.exists(&mut client)?;
        let (mut n, mut errors) = (0_i64, Vec::new());
        for (index, value) in deletes.iter().enumerate() {
            let result = (|| -> Result<_, CommandExecutionError> {
                let deletion = value.as_document().ok_or_else(|| {
                    CommandExecutionError::new("delete requires documents".into())
                })?;
                check_options(deletion, &["q", "limit"])?;
                let filter = deletion.get_document("q")?;
                let limit = integer(deletion.get("limit").ok_or_else(|| {
                    CommandExecutionError::new("delete limit is required".into())
                })?)?;
                if limit != 0 && limit != 1 {
                    return Err(CommandExecutionError::new(
                        "delete limit must be 0 or 1".into(),
                    ));
                }
                crate::parser::compile(filter)?;
                Ok(if exists {
                    client.delete(&sp, Some(filter), Some(limit as i32))?
                } else {
                    0
                })
            })();
            match result {
                Ok(count) => n += count as i64,
                Err(error) => {
                    errors
                        .push(doc! {"index":index as i32,"code":error.code,"errmsg":error.message});
                    if ordered {
                        break;
                    }
                }
            }
        }
        let mut reply = doc! {"ok":1.0,"n":n};
        if !errors.is_empty() {
            reply.insert("writeErrors", errors);
        }
        Ok(reply)
    }
}
