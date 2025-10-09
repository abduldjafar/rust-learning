use std::sync::Arc;

use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres};
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
    let pool: Arc<Pool<Postgres>> = Arc::new( PgPoolOptions::new()
    .max_connections(10)
    .connect("postgres://poatgres:poatgres@localhost/employee_activity").await?);

    let job4 = Job::new(
        "simple4",
        SourceKind::http("https://api.restful-api.dev/objects").build(),
        Sinker::postgres(
            pool,
            "public",
            "output3",
            true,
            false,
        ),
    );
    let (res2, res3, res4) = tokio::join!(job2.run(), job3.run(), job4.run());
    res2?;
    res3?;
    res4?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    info!("App starting...");
    run().await?;
    Ok(())
}
