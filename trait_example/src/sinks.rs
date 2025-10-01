use polars::prelude::*;
use std::fs::File;

pub trait Sink {
    fn save_data(&self, df: &DataFrame) -> PolarsResult<()>;
}

#[derive(Clone)]
pub enum Sinker {
    Csv(String),
}

impl Sink for Sinker {
    fn save_data(&self, df: &DataFrame) -> PolarsResult<()> {
        match self {
            Sinker::Csv(path) => {
                let mut file = File::create(path)?;
                // CsvWriter needs &mut DataFrame; clone to avoid mutating caller
                let mut tmp = df.clone();
                CsvWriter::new(&mut file).finish(&mut tmp)
            }
        }
    }
}