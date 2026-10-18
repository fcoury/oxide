use crate::{
    commands::{check_options, cursor_batch_size, Handler},
    handler::{CommandExecutionError, Request},
    pg::optional_bool,
};
use bson::{doc, Bson, Document};
pub struct ListCollections;
impl Handler for ListCollections {
    fn new() -> Self {
        Self
    }
    fn handle(
        &self,
        request: &Request,
        docs: &[Document],
    ) -> Result<Document, CommandExecutionError> {
        let doc = &docs[0];
        check_options(doc, &["listCollections", "nameOnly", "filter", "cursor"])?;
        let name_only = optional_bool(doc, "nameOnly", false)?;
        let name = match doc.get("filter") {
            None => None,
            Some(Bson::Document(filter)) if filter.is_empty() => None,
            Some(Bson::Document(filter)) if filter.len() == 1 && filter.contains_key("name") => {
                Some(filter.get_str("name")?)
            }
            _ => {
                return Err(CommandExecutionError::new(
                    "listCollections supports only an exact name filter".into(),
                ))
            }
        };
        let batch = cursor_batch_size(doc)?;
        let db = doc.get_str("$db")?;
        let collections=request.get_client()?.get_tables(db)?.into_iter().filter(|t|name.is_none_or(|n|n==t)).map(|name| {
            if name_only{doc!{"name":name,"type":"collection"}}else{doc!{"name":name,"type":"collection","options":{},"info":{"readOnly":false},"idIndex":{"name":"_id_","key":{"_id":1},"unique":true}}}
        }).collect();
        Ok(request
            .cursors
            .lock()
            .map_err(|_| CommandExecutionError::new("cursor lock unavailable".into()))?
            .first(
                format!("{db}.$cmd.listCollections"),
                collections,
                batch,
                false,
            )?)
    }
}
