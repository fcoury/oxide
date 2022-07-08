use crate::commands::Handler;
use crate::pg::PgDb;
use crate::serializer::PostgresSerializer;
use crate::wire::UnknownCommandError;
use bson::{doc, Bson, Document};

pub struct Insert {}

impl Handler for Insert {
    fn new() -> Self {
        Insert {}
    }

    fn handle(&self, docs: &Vec<Document>) -> Result<Document, UnknownCommandError> {
        let doc = &docs[0];
        let collection = doc.get_str("insert").unwrap();
        let db = doc.get_str("$db").unwrap();
        let docs = doc.get_array("documents").unwrap();
        let insert_doc = &docs[0];

        let mut client = PgDb::new();

        let bson: Bson = insert_doc.into();
        let json = bson.into_psql_json();
        let query = format!("INSERT INTO {}.{} VALUES ($1)", &db, &collection);

        client.exec(&query, &[&json]).unwrap();

        Ok(doc! {
          "ok": Bson::Double(1.0),
          "n": Bson::Int64(1),
          "lastErrorObject": doc! {
            "updatedExisting": Bson::Boolean(false),
            "n": Bson::Int64(1),
            "ok": Bson::Double(1.0),
          },
        })
    }
}
