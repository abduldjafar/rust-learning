use std::borrow::Cow;

use crate::errors::Result;
use polars::prelude::*;

pub trait Source {
    fn load_data(&self) -> Result<DataFrame>;
}

#[derive(Clone)]
pub enum SourceKind<'a> {
    Parquet(Cow<'a, str>),
    Postgres(Cow<'a, str>),
}

impl<'a> SourceKind<'a> {
    pub fn read_postgres(conn: impl Into<Cow<'a, str>>) -> Self {
        SourceKind::Postgres(conn.into())
    }

    pub fn read_parquet(path: impl Into<Cow<'a, str>>) -> Self {
        SourceKind::Parquet(path.into())
    }
}

impl<'a> Source for SourceKind<'a> {
    fn load_data(&self) -> Result<DataFrame> {
        match self {
            SourceKind::Parquet(path) => {
                let df = ParquetReader::new(std::fs::File::open(path.as_ref())?).finish()?;
                Ok(df)
            }
            SourceKind::Postgres(_name) => {
                let df = df![
                    "a" => &[1, 2, 3],
                    "b" => &[4, 5, 6]
                ]?;
                Ok(df)
            }
        }
    }
}
