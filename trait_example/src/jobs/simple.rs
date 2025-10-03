use crate::{
    jobs::SimpleJob,
    pipelines::{Pipeline, simple::SimplePipeline},
    sinks::Sinker,
    sources::SourceKind,
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
        let parquet_source = SourceKind::set_postgres("parquet".to_string());
        let sink = Sinker::Csv("output.csv".to_string());
        let pipeline = SimplePipeline::new(parquet_source, sink);
        pipeline.run().unwrap();
        Ok(())
    }
}
