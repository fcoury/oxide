use crate::commands::build_sql;
use crate::pg::{PgDb, SqlParam};
use bson::ser;
use nickel::{HttpRouter, JsonBody, MediaType, Nickel, Options};
use rust_embed::RustEmbed;
use serde_json::{json, Value};
use std::env;
use std::ffi::OsStr;
use std::path::Path;

#[derive(RustEmbed)]
#[folder = "public/"]
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
            log::info!("GET /index.html (static)");
            str.clone()
        },
    );

    server.post(
        "/convert",
        middleware! { |req, _res|
            let req_json = req.json_as::<Value>().unwrap();
            log::info!("POST /convert\n{:?}", req_json);
            let doc = ser::to_document(&req_json).unwrap();
            let sp = SqlParam::new(doc.get_str("database").unwrap(), doc.get_str("collection").unwrap());
            let sql = build_sql(&sp, doc.get_array("pipeline").unwrap()).unwrap();
            json!({ "sql": sql })
        },
    );

    server.post(
        "/run",
        middleware! { |req, _res|
            let req_json = req.json_as::<Value>().unwrap();
            let query = req_json["query"].as_str().unwrap();
            log::info!("POST /query\n{}", query);
            let mut client = PgDb::new();
            let mut rows = vec![];
            let res = client.raw_query(query, &[]);
            if res.is_err() {
                let err = res.unwrap_err();
                log::error!("{}", err);
                json!({ "error":err.to_string() })
            } else {
                for row in res.unwrap() {
                    let row: serde_json::Value = row.try_get::<&str, serde_json::Value>("_jsonb").unwrap();
                    rows.push(row);
                }
                json!({ "rows": rows })
            }
        },
    );

    server.get(
        "/databases",
        middleware! { |_req, _res|
            log::info!("GET /databases");
            let mut client = PgDb::new();
            let databases = client.get_schemas();
            json!({ "databases": databases })

        },
    );

    server.get(
        "/databases/:database/collections",
        middleware! { |req, _res|
            let database = req.param("database").unwrap();
            log::info!("GET /collections\ndatabase = {}", database);
            let mut client = PgDb::new();
            let collections = client.get_tables(database);
            json!({ "collections": collections })

        },
    );

    server.utilize(router! {
        get "**" => |req, mut res| {
            let file = req.path_without_query().unwrap().trim_start_matches("/");
            log::info!("GET /{} (static)", file);

            let html = Asset::get(file);
            match html {
                Some(html) => {
                    let html_str = std::str::from_utf8(html.data.as_ref()).unwrap();
                    let media_type = match Path::new(file).extension() {
                        Some(ext) => {
                            if ext == OsStr::new("html") {
                                MediaType::Html
                            } else if ext == OsStr::new("css") {
                                MediaType::Css
                            } else if ext == OsStr::new("js") {
                                MediaType::Js
                            } else {
                                MediaType::Txt
                            }
                        }
                        None => MediaType::Txt,
                    };
                    res.set(media_type);
                    format!("{}", html_str)
                }
                None => {
                    format!("{}", "404")
                }
            }
        }
    });

    log::info!("Web UI started at http://{}:{}...", listen_addr, port);
    server.listen(format!("{}:{}", listen_addr, port)).unwrap();
}
