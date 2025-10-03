use polars::prelude::*;

pub trait Source {
    fn load_data(&self) -> PolarsResult<DataFrame>;
}

#[derive(Clone)]
pub enum SourceKind {
    Parquet {
        name: String, /*, path: PathBuf */
    },
    Postgres {
        name: String, /*, dsn: String, query: String */
    },
}

impl SourceKind {
    pub fn read_postgres(name: String) -> Self {
        SourceKind::Parquet { name }
    }

    pub fn read_parquet(name: String) -> Self {
        SourceKind::Parquet { name }
    }
}

impl Source for SourceKind {
    fn load_data(&self) -> PolarsResult<DataFrame> {
        match self {
            SourceKind::Parquet { name: _ } => {
                let df = df![
                    "a" => &[1, 2, 3],
                    "b" => &[4, 5, 6]
                ]?;
                Ok(df)
            }
            SourceKind::Postgres { name: _ } => {
                let df = df![
                    "a" => &[1, 2, 3],
                    "b" => &[4, 5, 6]
                ]?;
                Ok(df)
            }
        }
    }
}
