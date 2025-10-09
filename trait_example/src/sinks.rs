use std::{
    borrow::Cow,
    fs::File,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use polars::prelude::*;
use sqlx::{postgres::PgPoolCopyExt, Acquire, Pool, Postgres};

use crate::errors::Result;

// ============================================================================
// Trait: Sink
// ============================================================================

/// Anything that can persist a `DataFrame`.
#[async_trait]
pub trait Sink {
    async fn save_data(&self, df: &mut DataFrame) -> Result<()>;
}

// ============================================================================
// Enum: Sinker
// ============================================================================

#[derive(Clone, Debug)]
pub enum Sinker<'a> {
    Csv(Cow<'a, str>),
    Parquet(Cow<'a, str>),
    Postgres {
        pool: Arc<Pool<Postgres>>,
        schema: Cow<'a, str>,
        table: Cow<'a, str>,
        auto_create: bool,
        upsert: bool,
        primary_key: Option<Cow<'a, str>>,
    },
}

impl<'a> Sinker<'a> {
    /// Create a CSV sinker.
    pub fn csv(path: impl Into<Cow<'a, str>>) -> Self {
        Self::Csv(path.into())
    }

    /// Create a Parquet sinker.
    pub fn parquet(path: impl Into<Cow<'a, str>>) -> Self {
        Self::Parquet(path.into())
    }

    /// Create a Postgres sinker.
    pub fn postgres(
        pool: Arc<Pool<Postgres>>,
        schema: impl Into<Cow<'a, str>>,
        table: impl Into<Cow<'a, str>>,
        auto_create: bool,
        upsert: bool,
    ) -> Self {
        Self::Postgres {
            pool,
            schema: schema.into(),
            table: table.into(),
            auto_create,
            upsert,
            primary_key: None,
        }
    }

    /// Optional helper to attach a primary key.
    pub fn with_primary_key(mut self, pk: impl Into<Cow<'a, str>>) -> Self {
        if let Sinker::Postgres { primary_key, .. } = &mut self {
            *primary_key = Some(pk.into());
        }
        self
    }
}

// ============================================================================
// Impl: Sink for Sinker
// ============================================================================

#[async_trait]
impl<'a> Sink for Sinker<'a> {
    async fn save_data(&self, df: &mut DataFrame) -> Result<()> {
        match self {
            Sinker::Csv(path) => {
                let mut file = File::create(path.as_ref())?;
                CsvWriter::new(&mut file)
                    .include_header(true)
                    .with_quote_style(QuoteStyle::Necessary)
                    .finish(df)?;
            }

            Sinker::Parquet(path) => {
                let mut file = File::create(path.as_ref())?;
                ParquetWriter::new(&mut file).finish(df)?;
            }

            Sinker::Postgres {
                pool,
                schema,
                table,
                auto_create,
                upsert,
                primary_key,
            } => {
                save_data_to_postgres(
                    df,
                    &**pool, // Arc<Pool<_>> -> &Pool<_>
                    schema.as_ref(),
                    table.as_ref(),
                    *auto_create,
                    *upsert,
                    primary_key.as_deref(),
                )
                .await?;
            }
        }
        Ok(())
    }
}

// ============================================================================
// Postgres Helpers
// ============================================================================

/// Double-quote an identifier and escape inner quotes.
fn q(id: &str) -> String {
    format!("\"{}\"", id.replace('"', "\"\""))
}

/// Create a table in Postgres if it doesn't already exist.
pub async fn create_table_if_not_exists(
    df: &DataFrame,
    pool: &Pool<Postgres>,
    schema: &str,
    table: &str,
    primary_key: Option<&str>,
) -> Result<()> {
    // Build `"col" TYPE` items
    let cols: Vec<String> = df
        .get_columns()
        .iter()
        .map(|s| -> Result<String> {
            let pg = polars_to_postgres_dtype(s.dtype())?;
            Ok(format!("{} {}", q(s.name()), pg))
        })
        .collect::<Result<_>>()?;

    let pk_clause = primary_key
        .map(|pk| format!(", PRIMARY KEY ({})", q(pk)))
        .unwrap_or_default();

    let sql = format!(
        "CREATE TABLE IF NOT EXISTS {}.{} ({cols}{pk})",
        q(schema),
        q(table),
        cols = cols.join(", "),
        pk = pk_clause
    );

    tracing::info!("Creating table: {sql}");
    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

// ============================================================================
// Data Type Mapping
// ============================================================================

/// Map Polars `DataType` to PostgreSQL type string.
pub fn polars_to_postgres_dtype(dtype: &DataType) -> Result<String> {
    use DataType::*;

    let ty = match dtype {
        // Integers (Postgres has no unsigned types)
        Int8 | Int16 => "smallint",
        Int32 => "int4",
        Int64 => "int8",
        UInt8 | UInt16 => "int4",
        UInt32 | UInt64 => "int8",

        // Floats
        Float32 => "float4",
        Float64 => "float8",

        // Booleans & Text/Binary
        Boolean => "boolean",
        String => "text",
        Binary => "bytea",

        // Dates & Times
        Date => "date",
        Time => "time",
        Datetime(_, tz) => {
            if tz.is_some() {
                "timestamptz"
            } else {
                "timestamp"
            }
        }
        Duration(_) => "interval",

        // Decimals
        Decimal => "numeric",

        // Nested / Complex → JSONB
        List(_) | Struct(_) => "jsonb",

        // Categories/Enums/Null → Text
        Categorical(_, _) | Enum(_, _) | Null => "text",

        // Catch-all
        _ => "text",
    };

    Ok(ty.to_string())
}

// ============================================================================
// Save Data to Postgres
// ============================================================================

async fn save_data_to_postgres(
    df: &mut DataFrame,
    pool: &Pool<Postgres>,
    schema: &str,
    table: &str,
    auto_create: bool,
    upsert: bool,
    primary_key: Option<&str>,
) -> Result<()> {
    // 1) Create table if needed
    if auto_create {
        create_table_if_not_exists(df, pool, schema, table, primary_key).await?;
    }

    // Collect column names once, in frame order
    let cols_df: Vec<String> = df
        .get_column_names_owned()
        .iter()
        .map(|c| c.to_string())
        .collect();
    let cols_quoted: Vec<String> = cols_df.iter().map(|c| q(c)).collect();

    if upsert {
        // ────────────────────────────────────────────────────────────────────
        // UPSERT path: stage -> copy -> insert on conflict
        // ────────────────────────────────────────────────────────────────────
        let pk = match primary_key {
            Some(pk) => pk,
            None => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "upsert requested but no primary key was provided",
                )
                .into());
            }
        };

        // Unique, process-local stage name
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let stage = format!("stage_{}_{}", table.replace('.', "_"), ts);

        // Use a transaction so stage + insert is atomic
        let mut tx = pool.begin().await?;

        // Create TEMP stage with same structure
        let create_stage = format!(
            "CREATE TEMP TABLE {stage} (LIKE {schema}.{table} INCLUDING ALL) ON COMMIT DROP",
            stage = q(&stage),
            schema = q(schema),
            table = q(table),
        );
        sqlx::query(&create_stage).execute(&mut *tx).await?;

        // COPY into stage
        let copy_sql = format!(
            "COPY {stage} ({cols}) FROM STDIN WITH (FORMAT csv)",
            stage = q(&stage),
            cols = cols_quoted.join(", "),
        );
        let conn = tx.acquire().await?; // -> &mut PgConnection
        let mut writer = conn.copy_in_raw(&copy_sql).await?;

        // Stream df -> csv bytes -> write
        const CHUNK: usize = 100_000;
        let height = df.height();
        for start in (0..height).step_by(CHUNK) {
            let len = (height - start).min(CHUNK);
            let chunk = df.slice(start as i64, len);
            let bytes = df_chunk_to_csv_bytes(chunk).await?;
            writer.send(bytes).await?;
        }
        writer.finish().await?;

        // Build UPDATE clause for non-PK columns
        let non_pk_sets = cols_df
            .iter()
            .filter(|c| c.as_str() != pk)
            .map(|c| format!("{} = EXCLUDED.{}", q(c), q(c)))
            .collect::<Vec<_>>()
            .join(", ");

        let insert_sql = format!(
            "INSERT INTO {schema}.{table} ({cols})
             SELECT {cols} FROM {stage}
             ON CONFLICT ({pk}) DO UPDATE SET {sets}",
            schema = q(schema),
            table = q(table),
            cols = cols_quoted.join(", "),
            stage = q(&stage),
            pk = q(pk),
            sets = non_pk_sets,
        );
        sqlx::query(&insert_sql).execute(&mut *tx).await?;
        tx.commit().await?;
    } else {
        // ────────────────────────────────────────────────────────────────────
        // Append path: direct COPY into target
        // ────────────────────────────────────────────────────────────────────
        let copy_sql = format!(
            "COPY {schema}.{table} ({cols}) FROM STDIN WITH (FORMAT csv)",
            schema = q(schema),
            table = q(table),
            cols = cols_quoted.join(", "),
        );
        let mut writer = pool.copy_in_raw(&copy_sql).await?;

        const CHUNK: usize = 100_000;
        let height = df.height();
        for start in (0..height).step_by(CHUNK) {
            let len = (height - start).min(CHUNK);
            let chunk = df.slice(start as i64, len);
            let bytes = df_chunk_to_csv_bytes(chunk).await?;
            writer.send(bytes).await?;
        }
        writer.finish().await?;
    }

    Ok(())
}

// ============================================================================
// CSV Chunk Conversion
// ============================================================================

/// Build CSV bytes for a DataFrame *chunk* off the main thread.
async fn df_chunk_to_csv_bytes(chunk: DataFrame) -> Result<Vec<u8>> {
    tokio::task::spawn_blocking(move || {
        let mut tmp = chunk.clone(); // CsvWriter needs &mut DataFrame
        let mut buf = Vec::with_capacity(tmp.height().saturating_mul(64));
        CsvWriter::new(&mut buf)
            .include_header(false) // COPY expects no header
            .with_quote_style(QuoteStyle::Necessary)
            .finish(&mut tmp)?;
        Ok(buf)
    })
    .await
    .expect("join blocking CSV task")
}