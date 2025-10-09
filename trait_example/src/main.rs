use std::sync::Arc;

use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres};
use tracing::info;
use trait_example::errors::Result;
use trait_example::{jobs::Job, sinks::Sinker, sources::SourceKind as source};

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
}
async fn setup_postgres_sink<'a>(
    schema: &'a str,
    table: &'a str,
    primary_key: &'a str,
    auto_create: bool,
    upsert: bool,
    pool: Arc<Pool<Postgres>>,

) -> Result<Sinker<'a>> {
    Ok(Sinker::postgres(
        pool,
        schema,
        table,
        auto_create,
        upsert,
        Some(primary_key.into()),
    ))
}
async fn run() -> Result<()> {


    let pool: Arc<Pool<Postgres>> = Arc::new( PgPoolOptions::new()
    .max_connections(10)
    .connect("postgres://poatgres:poatgres@localhost/employee_activity").await?);

    let example_source = source::http("https://api.restful-api.dev/objects").build();
    let example_source_2 = source::http("https://api.restful-api.dev/objects").build();

    let postgres_sink = setup_postgres_sink(    
        "public", 
        "output3", 
        "id", 
        true,
        true,
        pool.clone()
    ).await?;

    let postgres_sink_2 = setup_postgres_sink(    
        "public", 
        "output2", 
        "id", 
        true,
        true,
        pool.clone()
    ).await?;

    let job4 = Job::new(
        "simple4",
        example_source,
        postgres_sink,
    );

    let job5 = Job::new(
        "simple5",
        example_source_2,
        postgres_sink_2,
    );

    job4.run().await?;
    job5.run().await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    info!("App starting...");
    run().await?;
    Ok(())
}
