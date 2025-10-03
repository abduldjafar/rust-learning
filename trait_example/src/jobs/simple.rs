use crate::{ jobs::{Job, SimpleJob}, pipelines::{simple::SimplePipeline, Pipeline}, sinks::Sinker, sources::ParquetSource};


impl SimpleJob {
    pub fn set(name: String, schedule: String) -> Self {
        Self { name, schedule, status: "idle".to_string() }
    }

    pub(crate) fn start(&self) -> polars::prelude::PolarsResult<()> {
        let parquet_source = ParquetSource::new();
        let sink = Sinker::Csv("output.csv".to_string());
        let pipeline = SimplePipeline::new(parquet_source, sink);
        pipeline.run().unwrap();
        Ok(())
    }
    
    
}