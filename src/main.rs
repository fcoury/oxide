use clap::Parser;
use server::Server;

pub mod commands;
pub mod deserializer;
pub mod handler;
pub mod parser;
pub mod pg;
pub mod serializer;
pub mod server;
pub mod threadpool;
pub mod wire;

#[derive(Parser, Debug)]
#[clap(author, version, about)]
struct Args {
    #[clap(short, long)]
    listen_addr: Option<String>,

    #[clap(short, long)]
    port: Option<u16>,
}

fn main() {
    dotenv::dotenv().ok();
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );

    let args = Args::parse();
    let ip_addr = args.listen_addr.unwrap_or("127.0.0.1".to_string());
    let port = args.port.unwrap_or(27017);

    Server::new(ip_addr, port).start();
}
