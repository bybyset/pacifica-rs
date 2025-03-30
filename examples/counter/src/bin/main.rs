use clap::Parser;
use tracing::Level;
use tracing_subscriber::EnvFilter;
use pacifica_rs_example_counter::start_example_replica;

#[derive(Parser, Clone, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Opt {
    #[clap(long)]
    pub node_id: String,

    #[clap(long)]
    pub replica_group: String,

    #[clap(long)]
    pub storage_path: String,


}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Setup the logger
    tracing_subscriber::fmt()
        .with_target(true)
        .with_thread_ids(true)
        .with_level(true)
        .with_ansi(false)
        .with_max_level(Level::DEBUG)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let opt = Opt::parse();
    println!("pacifica-rs example: for counter single replica.");
    start_example_replica(opt.node_id, opt.replica_group, opt.storage_path).await
}
