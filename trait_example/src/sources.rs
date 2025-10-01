use polars::prelude::*;

pub trait Source {
    fn load_data(&self) -> PolarsResult<DataFrame>;
    fn get_name(&self) -> String;
}

#[derive(Clone)]
pub struct ParquetSource {
    name: String,
}

impl ParquetSource {
    pub fn new() -> Self {
        Self {
            name: "parquet".to_string(),
        }
    }
}

impl Source for ParquetSource {
    fn load_data(&self) -> PolarsResult<DataFrame> {
        let df = df![
            "a" => &[1, 2, 3],
            "b" => &[4, 5, 6]
        ]?;
        Ok(df)

        // Or real parquet:
        // LazyFrame::scan_parquet(&self.path, Default::default())?.collect()
    }

    fn get_name(&self) -> String {
        self.name.clone()
    }
}