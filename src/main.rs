use clap::Parser;
use indoc::indoc;
use server::Server;
use std::env;

pub mod commands;
pub mod deserializer;
pub mod handler;
pub mod parser;
pub mod pg;
pub mod serializer;
pub mod server;
pub mod threadpool;
pub mod utils;
pub mod wire;

#[derive(Parser, Debug)]
#[clap(author, version, about)]
struct Args {
    /// Listening address defaults to 127.0.0.1
    #[clap(short, long)]
    listen_addr: Option<String>,

    /// Listening port, defaults to 27017
    #[clap(short, long)]
    port: Option<u16>,

    /// PostgreSQL connection URL
    #[clap(short = 'u', long)]
    postgres_url: Option<String>,
}

fn main() {
    dotenv::dotenv().ok();
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );

    let args = Args::parse();
    let ip_addr = args
        .listen_addr
        .unwrap_or(env::var("OXIDE_LISTEN_ADDR").unwrap_or_else(|_| "127.0.0.1".to_string()));
    let port = args.port.unwrap_or(
        env::var("OXIDE_PORT")
            .unwrap_or("27017".to_string())
            .parse()
            .unwrap(),
    );
    let mut pg_url = args.postgres_url;
    if pg_url.is_none() {
        pg_url = env::var("DATABASE_URL").ok();
    }
    if let Some(pg_url) = pg_url {
        Server::new_with_pgurl(ip_addr, port, pg_url).start();
    } else {
        log::error!(indoc! {"
            No PostgreSQL URL specified.
            Use --postgres-url <url> or env var DATABASE_URL to set the connection URL and try again.
            For more information use --help.
        "});
    }
}
