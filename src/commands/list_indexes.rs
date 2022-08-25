use crate::handler::{CommandExecutionError, Request};
use crate::{commands::Handler, pg::SqlParam};
use bson::{doc, Bson, Document};
use regex::Regex;

pub struct ListIndexes {}

impl Handler for ListIndexes {
    fn new() -> Self {
        ListIndexes {}
    }

    fn handle(
        &self,
        request: &Request,
        docs: &Vec<Document>,
    ) -> Result<Document, CommandExecutionError> {
        let doc = &docs[0];
        let db = doc.get_str("$db").unwrap();
        let collection = doc.get_str("listIndexes").unwrap();
        let sp = SqlParam::new(db, collection);

        let mut client = request.get_client();
        let tables = client.get_tables(&sp.db);

        if !tables.contains(&collection.to_string()) {
            return Err(CommandExecutionError::new(format!(
                "Collection '{}' doesn't exist",
                collection
            )));
        }

        let mut indexes: Vec<Bson> = vec![];
        for table in tables {
            for row in &mut client.get_table_indexes(&sp.db, &table).unwrap() {
                let name: String = row.get("indexname");
                let def: String = row.get("indexdef");

                let mut keys: Document = doc! {};
                for field in parse_index_definition(def.as_str()) {
                    keys.insert(field, 1);
                }

                indexes.push(Bson::Document(doc! {
                    "v": 2,
                    "key": keys,
                    "name": name,
                }));
            }
        }

        return Ok(doc! {
            "cursor": doc! {
                "id": Bson::Int64(0),
                "ns": format!("{}.$cmd.listIndexes.{}", db, collection),
                "firstBatch": Bson::Array(indexes),
            },
            "ok": Bson::Double(1.0),
        });
    }
}

fn parse_index_definition(def: &str) -> Vec<String> {
    let regex = Regex::new(r"\s->\s'(.*?)'").unwrap();
    def.split("USING btree ")
        .nth(1)
        .unwrap()
        .split(", ")
        .map(|field| {
            regex
                .captures_iter(field)
                .map(|c| c[1].to_string())
                .collect::<Vec<_>>()
                .join(".")
        })
        .collect::<Vec<_>>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_nested_index_definition() {
        let def = r#"CREATE INDEX a_z_1_b_c_d_1 ON db_test."test_27edecea-7d1d-44c3-8443-98b100371df7" USING btree ((((_jsonb -> 'a'::text) -> 'z'::text)), ((((_jsonb -> 'b'::text) -> 'c'::text) -> 'd'::text)))"#;
        let keys = parse_index_definition(def);
        assert_eq!(&keys[0], "a.z");
        assert_eq!(&keys[1], "b.c.d");
    }

    #[test]
    fn test_parse_simple_index_definition() {
        let def = r#"REATE INDEX a_1_b_1 ON db_test."test_74885191-7780-4f29-9133-f2ced35cbc40" USING btree (((_jsonb -> 'a'::text)), ((_jsonb -> 'b'::text)))"#;
        let keys = parse_index_definition(def);
        assert_eq!(&keys[0], "a");
        assert_eq!(&keys[1], "b");
    }
}
