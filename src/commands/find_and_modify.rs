use crate::{
    commands::{check_options, Handler},
    handler::{CommandExecutionError, Request},
    parser::parse_update,
    pg::{optional_bool, SqlParam},
};
use bson::{doc, Bson, Document};
pub struct FindAndModify;
impl Handler for FindAndModify {
    fn new() -> Self {
        Self
    }
    fn handle(
        &self,
        request: &Request,
        docs: &[Document],
    ) -> Result<Document, CommandExecutionError> {
        let doc = &docs[0];
        check_options(
            doc,
            &[
                "findAndModify",
                "query",
                "sort",
                "update",
                "upsert",
                "new",
                "remove",
            ],
        )?;
        let sp = SqlParam::from(doc, "findAndModify")?;
        let query = match doc.get("query") {
            None => Document::new(),
            Some(Bson::Document(d)) => d.clone(),
            _ => {
                return Err(CommandExecutionError::new(
                    "query must be a document".into(),
                ))
            }
        };
        let sort = match doc.get("sort") {
            None => None,
            Some(Bson::Document(d)) => Some(d),
            _ => return Err(CommandExecutionError::new("sort must be a document".into())),
        };
        let upsert = optional_bool(doc, "upsert", false)?;
        let new = optional_bool(doc, "new", false)?;
        let mut client = request.get_client()?;
        if optional_bool(doc, "remove", false)? {
            if doc.contains_key("update") || upsert || new {
                return Err(CommandExecutionError::new(
                    "remove cannot be combined with update, upsert, or new".into(),
                ));
            }
            let value = client.find_and_remove(&sp, &query, sort)?;
            return Ok(
                doc! {"ok":1.0,"lastErrorObject":{"n":i32::from(value.is_some())},"value":value},
            );
        }
        let update = parse_update(doc.get_document("update")?)?;
        let result = client.update(&sp, Some(&query), sort, update, upsert, false)?;
        let mut last = doc! {"n":(result.matched+u64::from(result.upserted.is_some())) as i64,"updatedExisting":result.matched>0};
        if let Some(id) = result.upserted {
            last.insert("upserted", id);
        }
        Ok(doc! {"ok":1.0,"value":if new{result.after}else{result.before},"lastErrorObject":last})
    }
}
