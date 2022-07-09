use crate::handler::CommandExecutionError;
use crate::pg::PgDb;
use crate::{commands::Handler, pg::SqlParam};
use bson::{doc, Bson, Document};

pub struct Insert {}

impl Handler for Insert {
    fn new() -> Self {
        Insert {}
    }

    fn handle(&self, docs: &Vec<Document>) -> Result<Document, CommandExecutionError> {
        let doc = &docs[0];
        let db = doc.get_str("$db").unwrap();
        let collection = doc.get_str("insert").unwrap();
        let docs = doc.get_array("documents").unwrap();

        let mut client = PgDb::new();
        client.create_table_if_not_exists(db, collection).unwrap();

        let sp = SqlParam::new(db, collection);
        let inserted = client.insert_docs(sp, docs).unwrap();

        Ok(doc! {
          "ok": Bson::Double(1.0),
          "n": Bson::Int64(1),
          "lastErrorObject": doc! {
            "updatedExisting": Bson::Boolean(false),
            "n": Bson::Int64(inserted.try_into().unwrap()),
            "ok": Bson::Double(1.0),
          },
        })
    }
}
