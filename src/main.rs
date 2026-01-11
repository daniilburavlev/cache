use std::error::Error;

use clap::Parser;
use tokio::{net::TcpListener, signal};

mod cmd;
mod connection;
mod error;
mod parse;
mod server;
mod shutdown;
mod storage;

const DEFAULT_PORT: u16 = 6789;

pub type BoxedError = Box<dyn Error + Send + Sync>;

fn set_up_loggin() {
    tracing_subscriber::fmt::try_init().unwrap();
}

#[derive(Parser, Debug)]
#[command(name = "cache", version, about = "Cache server")]
struct Cli {
    #[arg(long)]
    port: Option<u16>,
}

#[tokio::main]
async fn main() -> Result<(), BoxedError> {
    set_up_loggin();

    let cli = Cli::parse();
    let port = cli.port.unwrap_or(DEFAULT_PORT);

    let listener = TcpListener::bind(&format!("127.0.0.1:{}", port)).await?;

    server::run(listener, signal::ctrl_c()).await;

    Ok(())
}
