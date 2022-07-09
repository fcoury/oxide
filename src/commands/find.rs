use crate::deserializer::PostgresJsonDeserializer;
use crate::pg::PgDb;
use crate::wire::UnknownCommandError;
use crate::{commands::Handler, pg::SqlParam};
use bson::{doc, Bson, Document};

pub struct Find {}

impl Handler for Find {
    fn new() -> Self {
        Find {}
    }

    fn handle(&self, docs: &Vec<Document>) -> Result<Document, UnknownCommandError> {
        let doc = &docs[0];
        let db = doc.get_str("$db").unwrap();
        let collection = doc.get_str("find").unwrap();
        let sp = SqlParam::new(db, collection);

        let mut client = PgDb::new();

        let rows = client.query("SELECT * FROM %table%", sp, &[]).unwrap();

        let mut res: Vec<Bson> = vec![];
        for row in rows.iter() {
            let json_value: serde_json::Value = row.get(0);
            let bson_value = json_value.from_psql_json();
            println!("{:?}", bson_value);
            res.push(bson_value);
        }

        println!("{:#?}", res);

        Ok(doc! {
            "ok": Bson::Double(1.0),
            "cursor": doc! {
                "id": Bson::Int64(0),
                "ns": format!("{}.{}", db, collection),
                "firstBatch": res,
            },
        })
    }
}
