use crate::{
    jobs::SimpleJob,
    pipelines::{Pipeline, simple::SimplePipeline},
    sinks::Sinker as sinker,
    sources::SourceKind as source,
};

impl SimpleJob {
    pub fn set(name: String, schedule: String) -> Self {
        Self {
            name,
            schedule,
            status: "idle".to_string(),
        }
    }

    pub(crate) fn start(&self) -> polars::prelude::PolarsResult<()> {
        let parquet_source = source::read_parquet("parquet".to_string());
        let sink = sinker::write_csv("output.csv".to_string());
        let pipeline = SimplePipeline::new(parquet_source, sink);
        pipeline.run().unwrap();
        Ok(())
    }
}
