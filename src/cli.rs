use clap::{AppSettings, Parser, Subcommand};
use serde::Serialize;

#[derive(Subcommand, Debug, Clone, Serialize)]
pub enum Commands {
    /// Start OxideDB web interface
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
}

#[derive(Parser, Debug, Clone, Serialize)]
#[clap(author, version, about)]
#[clap(global_setting(AppSettings::ArgsNegateSubcommands))]
pub struct Cli {
    #[clap(subcommand)]
    pub command: Option<Commands>,

    /// Listening address, defaults to 127.0.0.1
    #[clap(short, long)]
    pub listen_addr: Option<String>,

    /// Listening port, defaults to 27017
    #[clap(short, long)]
    pub port: Option<u16>,

    /// PostgreSQL connection URL
    #[clap(short = 'u', long)]
    pub postgres_url: Option<String>,

    /// Starts web interface
    #[clap(short, long)]
    pub web: bool,

    /// Web binding address
    #[clap(long)]
    pub web_addr: Option<String>,

    /// Show debugging information
    #[clap(short, long)]
    pub debug: bool,

    /// Enables tracing commands
    #[clap(short, long)]
    pub trace: bool,
}
