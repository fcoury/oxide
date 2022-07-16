use crate::parser::parse;
use crate::pg::PgDb;
use bson::ser;
use nickel::{HttpRouter, JsonBody, Nickel, Options};
use rust_embed::RustEmbed;
use serde_json::{json, Value};
use std::env;

#[derive(RustEmbed)]
#[folder = "assets/"]
struct Asset;

pub fn start(listen_addr: &str, port: u16, postgres_url: Option<String>) {
    let mut server = Nickel::new();
    server.options = Options::default().output_on_listen(false);

    let mut pg_url = postgres_url;
    if pg_url.is_none() {
        pg_url = env::var("DATABASE_URL").ok();
    }
    if pg_url.is_none() {
        log::error!(indoc::indoc! {"
                No PostgreSQL URL specified.
                Use --postgres-url <url> or env var DATABASE_URL to set the connection URL and try again.
                For more information use --help.
            "});
    }

    let index_html = Asset::get("index.html").unwrap();
    let index_data = std::str::from_utf8(index_html.data.as_ref());
    let str = format!("{}", index_data.unwrap());

    server.get(
        "/",
        middleware! { |_req, _res|
            str.clone()
        },
    );

    server.post(
        "/convert",
        middleware! { |req, _res|
            let req_json = req.json_as::<Value>().unwrap();
            println!("{:?}", req_json);
            let doc = ser::to_document(&req_json).unwrap();
            let sql = parse(doc);
            json!({ "sql": sql })

        },
    );

    server.get(
        "/databases",
        middleware! { |_req, _res|
            let mut client = PgDb::new();
            let databases = client.get_schemas();
            json!({ "databases": databases })

        },
    );

    server.get(
        "/databases/:database/collections",
        middleware! { |req, _res|
            let database = req.param("database").unwrap();
            let mut client = PgDb::new();
            let collections = client.get_tables(database);
            json!({ "collections": collections })

        },
    );

    log::info!("Web UI started at http://{}:{}...", listen_addr, port);
    server.listen(format!("{}:{}", listen_addr, port)).unwrap();
}
