use tracing::info;
use trait_example::errors::Result;
use trait_example::{jobs::Job, sinks::Sinker, sources::SourceKind};

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();
}

fn run() -> Result<()> {
    let job = Job::new(
        "simple",
        SourceKind::read_parquet("parquet"),
        Sinker::csv("output.csv"),
    );
    job.run()
}

fn main() -> Result<()> {
    init_tracing();
    info!("App starting...");
    run()
}
