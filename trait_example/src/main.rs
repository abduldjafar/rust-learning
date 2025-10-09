use tracing::info;
use trait_example::errors::Result;
use trait_example::{jobs::Job, sinks::Sinker, sources::SourceKind};

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
}

async fn run() -> Result<()> {

    let job2 = Job::new(
        "simple2",
        SourceKind::read_postgres("postgres"),
        Sinker::parquet("output.parquet"),
    );

    let job3 = Job::new(
        "simple3",
        SourceKind::http("https://api.restful-api.dev/objects").build(),
        Sinker::csv("output2.csv"),
    );

    job2.run().await?;
    job3.run().await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    info!("App starting...");
    run().await?;
    Ok(())
}
