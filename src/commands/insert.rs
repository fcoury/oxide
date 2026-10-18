use crate::{
    commands::{check_options, Handler},
    handler::{CommandExecutionError, Request},
    pg::{optional_bool, SqlParam},
};
use bson::{doc, Document};
pub struct Insert;
impl Handler for Insert {
    fn new() -> Self {
        Self
    }
    fn handle(
        &self,
        request: &Request,
        docs: &[Document],
    ) -> Result<Document, CommandExecutionError> {
        let doc = &docs[0];
        check_options(doc, &["insert", "documents", "ordered"])?;
        let sp = SqlParam::from(doc, "insert")?;
        let values = doc.get_array("documents")?;
        if values.is_empty() {
            return Err(CommandExecutionError::new(
                "documents cannot be empty".into(),
            ));
        }
        if doc.get_array("documents")?.len() > 10_000 {
            return Err(CommandExecutionError::new(
                "write batch exceeds 10000 entries".into(),
            ));
        }
        let ordered = optional_bool(doc, "ordered", true)?;
        let mut client = request.get_client()?;
        client.create_table_if_not_exists(&sp.db, &sp.collection)?;
        let mut n = 0_i64;
        let mut errors = Vec::new();
        for (index, value) in values.iter().enumerate() {
            let result = (|| -> Result<_, CommandExecutionError> {
                let doc = value.as_document().ok_or_else(|| {
                    CommandExecutionError::new("insert requires documents".into())
                })?;
                Ok(client.insert_doc(sp.clone(), doc)?)
            })();
            match result {
                Ok(_) => n += 1,
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
