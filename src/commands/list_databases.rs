use crate::commands::Handler;
use crate::wire::UnknownCommandError;
use bson::{doc, Bson, Document};
use postgres::{Client, NoTls};

pub struct ListDatabases {}

fn get_schemas(client: &mut Client) -> Vec<String> {
    let mut schemas = Vec::new();
    let rows = client
        .query(
            "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name",
            &[],
        )
        .unwrap();
    for row in rows.iter() {
        let schema_name: &str = row.get(0);
        if schema_name.starts_with("pg_") || schema_name != "information_schema" {
            schemas.push(row.get(0));
        }
    }
    schemas
}

fn get_tables(client: &mut Client, database: &str) -> Vec<String> {
    let mut tables = Vec::new();
    let rows = client
        .query(
            "
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = $1
      GROUP BY table_name
      ORDER BY table_name
      ",
            &[&database],
        )
        .unwrap();
    for row in rows.iter() {
        tables.push(row.get(0));
    }
    tables
}

fn get_table_size(client: &mut Client, schema: &str, table: &str) -> i64 {
    let row = client
        .query_one(
            format!("SELECT pg_relation_size('{}.{}')", schema, table).as_str(),
            &[],
        )
        .unwrap();

    row.get(0)
}

impl Handler for ListDatabases {
    fn new() -> Self {
        ListDatabases {}
    }

    fn handle(&self, _doc: Document) -> Result<Document, UnknownCommandError> {
        let mut client =
            Client::connect("postgresql://postgres:postgres@localhost/ferretdb", NoTls).unwrap();

        let mut total_size: i64 = 0;
        let mut databases: Vec<bson::Bson> = vec![];
        for schema in get_schemas(&mut client) {
            if schema.starts_with("pg_") || schema == "information_schema" {
                continue;
            }
            let mut size: i64 = 0;
            for table in get_tables(&mut client, &schema) {
                let db_size = get_table_size(&mut client, &schema, &table);
                size += db_size;
                total_size += db_size;
            }
            let empty = size <= 0;
            databases.push(doc!["name": schema, "sizeOnDisk": size, "empty": empty].into());
        }

        let databases_doc = Bson::Array(databases);
        Ok(doc! {
          "databases": databases_doc,
          "totalSize": Bson::Int64(total_size),
          "totalSizeMb": Bson::Int64(total_size/1024/1024),
          "ok": Bson::Double(1.0),
        })
    }
}
