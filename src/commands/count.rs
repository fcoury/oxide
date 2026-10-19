use crate::{
    commands::{check_options, Handler},
    handler::{CommandExecutionError, Request},
    parser,
    pg::{integer, SqlParam},
};
use bson::{doc, Bson, Document};
pub struct Count;
impl Handler for Count {
    fn new() -> Self {
        Self
    }
    fn handle(
        &self,
        request: &Request,
        docs: &[Document],
    ) -> Result<Document, CommandExecutionError> {
        let doc = &docs[0];
        check_options(doc, &["count", "query", "skip", "limit"]).map_err(|mut error| {
            if error.message.starts_with("unsupported option") {
                error.code = 40415;
                error.code_name = "IDLUnknownField".into();
            }
            error
        })?;
        let sp = SqlParam::from(doc, "count")?;
        let filter = match doc.get("query") {
            None => Document::new(),
            Some(Bson::Document(d)) => d.clone(),
            _ => {
                return Err(CommandExecutionError::new(
                    "query must be a document".into(),
                ))
            }
        };
        let mut sql = parser::compile(&filter)?;
        let skip = doc.get("skip").map(integer).transpose()?.unwrap_or(0);
        let limit = doc
            .get("limit")
            .map(integer)
            .transpose()?
            .unwrap_or(0)
            .checked_abs()
            .ok_or_else(|| CommandExecutionError::new("limit is out of range".into()))?;
        if skip < 0 {
            return Err(CommandExecutionError::new(
                "skip must be nonnegative".into(),
            ));
        }
        let mut pg = request.get_client()?;
        let count: i64 = if sp.exists(&mut pg)? {
            sql.text = format!(
                "SELECT COUNT(*) FROM (SELECT 1 FROM {sp} WHERE {} {} OFFSET {skip}) AS counted",
                sql.text,
                if limit == 0 {
                    String::new()
                } else {
                    format!("LIMIT {limit}")
                }
            );
            pg.raw_query(&sql.text, &sql.refs())?[0]
                .try_get(0)
                .map_err(eyre::Report::from)?
        } else {
            0
        };
        let count = i32::try_from(count)
            .map(Bson::Int32)
            .unwrap_or(Bson::Int64(count));
        Ok(doc! {"ok":1.0,"n":count})
    }
}
