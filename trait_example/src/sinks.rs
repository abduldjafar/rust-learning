use polars::prelude::*;
use std::{borrow::Cow, fs::File};

pub trait Sink {
    fn save_data(&self, df: &DataFrame) -> PolarsResult<()>;
}

#[derive(Clone, Debug)]
pub enum Sinker<'a> {
    Csv(Cow<'a, str>),
    Parquet(Cow<'a, str>),
}

impl<'a> Sinker<'a> {
    pub fn csv(path: impl Into<Cow<'a, str>>) -> Self {
        Self::Csv(path.into())
    }
    pub fn parquet(path: impl Into<Cow<'a, str>>) -> Self {
        Self::Parquet(path.into())
    }
}

impl<'a> Sink for Sinker<'a> {
    fn save_data(&self, df: &DataFrame) -> PolarsResult<()> {
        match self {
            Sinker::Csv(path) => {
                let mut file = File::create(path.as_ref())?;
                // CsvWriter needs &mut DataFrame; clone to avoid mutating caller
                let mut tmp = df.clone();
                CsvWriter::new(&mut file).finish(&mut tmp)
            }
            Sinker::Parquet(path) => {
                let mut file = File::create(path.as_ref())?;
                let mut tmp = df.clone();
                ParquetWriter::new(&mut file).finish(&mut tmp)?;
                Ok(())
            }
        }
    }
}
