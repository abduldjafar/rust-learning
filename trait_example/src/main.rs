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
    primary_key: Option<&'a str>,
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
        primary_key.map(std::borrow::Cow::from),
    ))
}
async fn run() -> Result<()> {


    let pool: Arc<Pool<Postgres>> = Arc::new( PgPoolOptions::new()
    .max_connections(10)
    .connect("postgres://poatgres:poatgres@localhost/employee_activity").await?);

    let example_source_3 = source::http("https://intranet.paysera.net/rest/api/search")
    .query("cql", "lastModified>='2025-01-01' AND lastModified<='2025-01-03' AND type IN (page, blogpost, comment, attachment)")
    .query("expand", "content.version")
    .query("limit", "10000")
    .query("start", "0")
    .header("Authorization", "Bearer iX6cjTizXTBPGQmgyaeTcmA34p3nbwuRKLP")
    .header("Content-Type", "application/json")
    .build();



    let postgres_sink_3 = setup_postgres_sink(    
        "public", 
        "rust_confluence", 
        None, 
        true,
        false,
        pool.clone()
    ).await?;

    let job6 = Job::new(
        "rust_confluence",
        example_source_3,
        postgres_sink_3,
    ).with_operation(|df| {
        use polars::prelude::*;
        use tracing::{info, warn};
        use std::collections::HashSet;
    
        info!("=== Starting Automatic Unnesting ===");
        
        // Initial processing
        let mut result = df.clone()
            .lazy()
            .explode(Selector::ByName { names: Arc::new(["results".into()]), strict: true })
            .collect()?;
        
        info!("After explode: {:?}", result.shape());
        
        // First unnest of results
        result = result
            .lazy()
            .unnest(Selector::ByName { names: Arc::new(["results".into()]), strict: true })
            .collect()?;
        
        info!("After results unnest: {:?}", result.shape());
        info!("Columns: {:?}", result.get_column_names());
        
        // Automatic recursive unnesting with conflict resolution
        let mut iteration = 0;
        let max_iterations = 10; // Safety limit
        
        loop {
            iteration += 1;
            
            if iteration > max_iterations {
                warn!("Max iterations reached, stopping unnesting");
                break;
            }
            
            // Find struct columns
            let struct_info: Vec<(String, Vec<String>)> = result
                .get_columns()
                .iter()
                .filter_map(|c| {
                    if let DataType::Struct(fields) = c.dtype() {
                        let field_names: Vec<String> = fields
                            .iter()
                            .map(|f| f.name.to_string())
                            .collect();
                        Some((c.name().to_string(), field_names))
                    } else {
                        None
                    }
                })
                .collect();
            
            if struct_info.is_empty() {
                info!("No more struct columns to process");
                break;
            }
            
            info!("Iteration {}: Found {} struct columns", iteration, struct_info.len());
            
            // Process first struct
            let (struct_name, fields) = &struct_info[0];
            info!("  Processing: {} with {} fields", struct_name, fields.len());
            
            // Get existing columns (excluding current struct)
            let existing: HashSet<String> = result
                .get_column_names()
                .iter()
                .map(|s| s.to_string())
                .filter(|s| s != struct_name)
                .collect();
            
            // Resolve conflicts
            let mut rename_count = 0;
            for field_name in fields {
                if existing.contains(field_name) {
                    let new_name = format!("{}_from_{}", 
                        field_name, 
                        struct_name.replace("_", "")
                    );
                    
                    info!("    Renaming conflict: {} -> {}", field_name, new_name);
                    
                    result.rename(field_name, (&new_name).into())
                        .map_err(|e| {
                            warn!("Failed to rename {}: {}", field_name, e);
                            e
                        })?; 
                    rename_count += 1;
                }
            }
            
            if rename_count > 0 {
                info!("    Resolved {} conflicts", rename_count);
            }
            
            // Unnest the struct
            info!("    Unnesting {}...", struct_name);
            result = result
                .lazy()
                .unnest(Selector::ByName { names: Arc::new([struct_name.into()]), strict: true })
                .collect()
                .map_err(|e| {
                    warn!("Failed to unnest {}: {}", struct_name, e);
                    e
                })?;
            
            info!("    After unnest: {} columns", result.width());
        }
        
        // Drop any remaining list/struct columns
        let complex_cols: Vec<String> = result
            .get_columns()
            .iter()
            .filter_map(|c| {
                if matches!(c.dtype(), DataType::List(_) | DataType::Struct(_)) {
                    Some(c.name().to_string())
                } else {
                    None
                }
            })
            .collect();
        
        if !complex_cols.is_empty() {
            info!("Dropping remaining complex columns: {:?}", complex_cols);
            for col_name in complex_cols {
                result = result.drop(&col_name)?;
            }
        }
        
        Ok(result)
    }).with_operation(|df| {
        use polars::prelude::*;
        use tracing::info;
    
        info!("=== Cleaning NULL bytes from text columns ===");
        
        // Get all string columns
        let string_columns: Vec<String> = df
            .get_columns()
            .iter()
            .filter_map(|c| {
                if matches!(c.dtype(), DataType::String) {
                    Some(c.name().to_string())
                } else {
                    None
                }
            })
            .collect();
        
        info!("Found {} string columns to clean", string_columns.len());
        
        // Clean each string column
        let mut cleaned = df.clone();
        for col_name in &string_columns {
            info!("  Cleaning column: {}", col_name);
            
            cleaned = cleaned
                .lazy()
                .with_column(
                    col(col_name)
                        .str()
                        .replace_all(lit("\0"), lit(""), true)  // ✅ Remove NULL bytes
                        .alias(col_name)
                )
                .collect()?;
        }
        
        info!("Cleaned {} columns", string_columns.len());
        
        Ok(cleaned)
    });

    job6.run().await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    info!("App starting...");
    run().await?;
    Ok(())
}
