use crate::cli::Cli;
use crate::commands::build_sql;
use crate::pg::{PgDb, SqlParam};
use bson::ser;
use nickel::{HttpRouter, JsonBody, MediaType, Nickel, Options};
use rust_embed::RustEmbed;
use serde_json::{json, Value};
use std::env;
use std::path::Path;

#[derive(RustEmbed)]
#[folder = "public/"]
struct Asset;

pub fn start(listen_addr: &str, port: u16, postgres_url: Option<String>, cli: Cli) {
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
    let pg_url = pg_url.unwrap();

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

    server.get(
        "/api/config",
        middleware! { |_req, _res|
            log::info!("GET /api/config");
            json!({
                "version": env!("CARGO_PKG_VERSION"),
                "cli": cli,
            })
        },
    );

    server.post(
        "/api/convert",
        middleware! { |req, _res|
            let req_json = req.json_as::<Value>().unwrap();
            log::info!("POST /api/convert\n{:?}", req_json);
            let doc = ser::to_document(&req_json).unwrap();
            let sp = SqlParam::new(doc.get_str("database").unwrap(), doc.get_str("collection").unwrap());
            let res = build_sql(&sp, doc.get_array("pipeline").unwrap());
            if res.is_err() {
                let err = res.unwrap_err();
                log::error!("{}", err);
                json!({ "error":err.to_string() })
            } else {
                let sql = res.unwrap();
                json!({ "sql": sql })
            }

        },
    );

    let uri = pg_url.clone();
    server.post(
        "/api/run",
        middleware! { |req, _res|
            let req_json = req.json_as::<Value>().unwrap();
            let query = req_json["query"].as_str().unwrap();
            log::info!("POST /api/query\n{}", query);
            let mut client = PgDb::new_with_uri(&uri);
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

    let uri = pg_url.clone();
    server.get(
        "/api/databases",
        middleware! { |_req, _res|
            log::info!("GET /api/databases");
            let mut client = PgDb::new_with_uri(&uri);
            let databases = client.get_schemas();
            json!({ "databases": databases })

        },
    );

    let uri = pg_url.clone();
    server.get(
        "/api/databases/:database/collections",
        middleware! { |req, _res|
            let database = req.param("database").unwrap();
            log::info!("GET /api/collections\ndatabase = {}", database);
            let mut client = PgDb::new_with_uri(&uri);
            let collections = client.get_tables(database);
            json!({ "collections": collections })

        },
    );

    let uri = pg_url.clone();
    server.get(
        "/api/traces",
        middleware! { |_req, _res|
            log::info!("GET /api/traces");
            let mut client = PgDb::new_with_uri(&uri);
            let traces = client.get_traces();
            json!({ "traces": traces })
        },
    );

    server.utilize(router! {
        get "/**" => |req, mut res| {
            let uri = req.path_without_query().unwrap();
            let file = uri.trim_start_matches("/");
            log::info!("GET /{} (static)", file);

            let media_type = mime_from_filename(file).unwrap_or(MediaType::Html);
            res.set(media_type);

            let contents = Asset::get(file);
            contents.unwrap().data.as_ref()
        }
    });

    log::info!("Web UI started at http://{}:{}...", listen_addr, port);
    server.listen(format!("{}:{}", listen_addr, port)).unwrap();
}

fn mime_from_filename<P: AsRef<Path>>(path: P) -> Option<MediaType> {
    path.as_ref()
        .extension()
        .and_then(|os| os.to_str())
        // Lookup mime from file extension
        .and_then(|s| s.parse().ok())
}
