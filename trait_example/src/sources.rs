use std::borrow::Cow;

use polars::prelude::*;

pub trait Source {
    fn load_data(&self) -> PolarsResult<DataFrame>;
}

#[derive(Clone)]
pub enum SourceKind<'a> {
    Parquet(Cow<'a, str>),
    Postgres(Cow<'a, str>),
}

impl<'a> SourceKind<'a> {
    pub fn read_postgres(name: impl Into<Cow<'a, str>>) -> Self {
        SourceKind::Postgres(name.into())
    }

    pub fn read_parquet(name: impl Into<Cow<'a, str>>) -> Self {
        SourceKind::Parquet(name.into())
    }
}

impl<'a> Source for SourceKind<'a> {
    fn load_data(&self) -> PolarsResult<DataFrame> {
        match self {
            SourceKind::Parquet(_name) => {
                let df = df![
                    "a" => &[1, 2, 3],
                    "b" => &[4, 5, 6]
                ]?;
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
