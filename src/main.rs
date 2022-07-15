use clap::{AppSettings, Parser, Subcommand};
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

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start OxideDB web interface
    Web {
        /// Listening address, defaults to 127.0.0.1
        #[clap(short, long)]
        listen_addr: Option<String>,

        /// Listening port, defaults to 8087
        #[clap(short, long)]
        port: Option<u16>,
    },
}

#[derive(Parser, Debug)]
#[clap(author, version, about)]
#[clap(global_setting(AppSettings::ArgsNegateSubcommands))]
struct Cli {
    #[clap(subcommand)]
    command: Option<Commands>,

    /// Listening address, defaults to 127.0.0.1
    #[clap(short, long)]
    listen_addr: Option<String>,

    /// Listening port, defaults to 27017
    #[clap(short, long)]
    port: Option<u16>,

    /// PostgreSQL connection URL
    #[clap(short = 'u', long)]
    postgres_url: Option<String>,

    /// Show debugging information
    #[clap(short, long)]
    debug: bool,
}

fn main() {
    dotenv::dotenv().ok();

    let cli = Cli::parse();

    let log_level = if cli.debug { "oxide=debug" } else { "info" };
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, log_level),
    );

    match cli.command {
        Some(Commands::Web { listen_addr, port }) => println!("{:?}, {:?}", listen_addr, port),
        None => {
            start(cli.listen_addr, cli.port, cli.postgres_url);
        }
    }

    fn start(listen_addr: Option<String>, port: Option<u16>, postgres_url: Option<String>) {
        let ip_addr = listen_addr
            .unwrap_or(env::var("OXIDE_LISTEN_ADDR").unwrap_or_else(|_| "127.0.0.1".to_string()));
        let port = port.unwrap_or(
            env::var("OXIDE_PORT")
                .unwrap_or("27017".to_string())
                .parse()
                .unwrap(),
        );
        let mut pg_url = postgres_url;
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
}
