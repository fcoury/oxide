use clap::{AppSettings, Parser, Subcommand};
use indoc::indoc;
use server::Server;
use std::env;
use std::thread;

#[macro_use]
extern crate nickel;

pub mod commands;
pub mod deserializer;
pub mod handler;
pub mod parser;
pub mod pg;
pub mod serializer;
pub mod server;
pub mod shell;
pub mod threadpool;
pub mod ui;
pub mod utils;
pub mod wire;

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start web interface
    Web {
        /// Listening address, defaults to 127.0.0.1
        #[clap(short, long)]
        listen_addr: Option<String>,

        /// Listening port, defaults to 8087
        #[clap(short, long)]
        port: Option<u16>,

        /// PostgreSQL connection URL
        #[clap(short = 'u', long)]
        postgres_url: Option<String>,
    },

    /// Start JavaScript shell
    Shell {
        /// Server address
        #[clap(short = 'l', long, default_value_t = String::from("127.0.0.1"))]
        server_addr: String,

        /// Server port
        #[clap(short = 'p', long, default_value_t = 27017)]
        server_port: u16,
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

    /// Starts web interface
    #[clap(short, long)]
    web: bool,

    /// Web binding address
    #[clap(long)]
    web_addr: Option<String>,

    /// Show debugging information
    #[clap(short, long)]
    debug: bool,
}

fn main() {
    dotenv::dotenv().ok();

    let cli = Cli::parse();

    let log_level = if cli.debug {
        "oxide=debug"
    } else {
        "oxide=info"
    };
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, log_level),
    );

    match cli.command {
        Some(Commands::Web {
            listen_addr,
            port,
            postgres_url,
        }) => {
            ui::start(
                &listen_addr.unwrap_or("localhost".to_string()),
                port.unwrap_or(8087),
                postgres_url,
            );
        }
        Some(Commands::Shell {
            server_addr,
            server_port,
        }) => {
            shell::start(&server_addr, server_port);
        }
        None => {
            start(
                cli.listen_addr,
                cli.port,
                cli.postgres_url,
                cli.web,
                cli.web_addr,
            );
        }
    }

    fn start(
        listen_addr: Option<String>,
        port: Option<u16>,
        postgres_url: Option<String>,
        web: bool,
        web_addr: Option<String>,
    ) {
        let ip_addr = listen_addr
            .unwrap_or(env::var("OXIDE_LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0".to_string()));
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
            if web || web_addr.is_some() {
                let pg_url_clone = pg_url.clone();
                let parts = web_addr.unwrap_or(
                    env::var("OXIDE_WEB_ADDR").unwrap_or_else(|_| "0.0.0.0:8087".to_string()),
                );
                let parts_vec = parts.split(':').collect::<Vec<_>>();
                let web_addr = parts_vec[0].to_string();
                let port = parts_vec[1].parse::<u16>().unwrap_or(8087);
                thread::spawn(move || {
                    ui::start(&web_addr, port, Some(pg_url_clone));
                });
            }

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
