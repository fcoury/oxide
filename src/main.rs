use clap::{Parser, Subcommand};
use eyre::{eyre, Result, WrapErr};
use oxide::server::Server;
use std::env;

#[derive(Subcommand, Debug)]
enum Commands {
    #[cfg(feature = "web")]
    /// Start the local pipeline workbench (requires OXIDE_WEB_TOKEN)
    Web {
        #[arg(short, long, default_value = "127.0.0.1")]
        listen_addr: std::net::IpAddr,
        #[arg(short, long, default_value_t = 8087)]
        port: u16,
        #[arg(short = 'u', long)]
        postgres_url: Option<String>,
    },
    #[cfg(feature = "shell")]
    /// Launch the installed MongoDB Shell (mongosh)
    Shell {
        #[arg(short = 'l', long, default_value = "127.0.0.1")]
        server_addr: String,
        #[arg(short = 'p', long, default_value_t = 27017)]
        server_port: u16,
    },
}

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
    /// Listening address (default: 127.0.0.1; env: OXIDE_LISTEN_ADDR)
    #[arg(short, long)]
    listen_addr: Option<String>,
    /// Listening port (default: 27017; env: OXIDE_PORT)
    #[arg(short, long)]
    port: Option<u16>,
    /// PostgreSQL connection URL (env: DATABASE_URL)
    #[arg(short = 'u', long)]
    postgres_url: Option<String>,
    /// Show debugging information
    #[arg(short, long)]
    debug: bool,
}

fn postgres_url(value: Option<String>) -> Result<String> {
    value
        .or_else(|| env::var("DATABASE_URL").ok())
        .ok_or_else(|| eyre!("set --postgres-url or DATABASE_URL"))
}

fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let cli = Cli::parse();
    let level = if cli.debug {
        "oxide=debug"
    } else {
        "oxide=info"
    };
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, level),
    );
    match cli.command {
        #[cfg(feature = "web")]
        Some(Commands::Web {
            listen_addr,
            port,
            postgres_url: url,
        }) => oxide::ui::start((listen_addr, port).into(), &postgres_url(url)?),
        #[cfg(feature = "shell")]
        Some(Commands::Shell {
            server_addr,
            server_port,
        }) => oxide::shell::start(&server_addr, server_port),
        None => {
            let host = cli
                .listen_addr
                .or_else(|| env::var("OXIDE_LISTEN_ADDR").ok())
                .unwrap_or_else(|| "127.0.0.1".into());
            let port = match cli.port {
                Some(port) => port,
                None => env::var("OXIDE_PORT")
                    .unwrap_or_else(|_| "27017".into())
                    .parse()
                    .wrap_err("OXIDE_PORT must be a TCP port")?,
            };
            Server::new_with_pgurl(host, port, postgres_url(cli.postgres_url)?).try_start()
        }
    }
}
