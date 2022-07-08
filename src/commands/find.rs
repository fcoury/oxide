use crate::commands::Handler;
use crate::wire::UnknownCommandError;
use bson::{doc, Bson, Document};
use postgres::{Client, NoTls};

pub struct Find {}

impl Handler for Find {
    fn new() -> Self {
        Find {}
    }

    fn handle(&self, docs: &Vec<Document>) -> Result<Document, UnknownCommandError> {
        let doc = &docs[0];
        let collection = doc.get_str("find").unwrap();
        let db = doc.get_str("$db").unwrap();

        let mut client =
            Client::connect("postgresql://postgres:postgres@localhost/ferretdb", NoTls).unwrap();

        let query = format!("SELECT * FROM {}.{}", &db, &collection);
        let rows = client.query(&query, &[]).unwrap();

        let mut res: Vec<Bson> = vec![];
        for row in rows.iter() {
            let json_value: serde_json::Value = row.get(0);
            let bson_value = bson::to_bson(&json_value).unwrap();
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
