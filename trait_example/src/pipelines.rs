use polars::{error::PolarsResult, frame::DataFrame};
use crate::{sinks::{Sink, Sinker}, sources::ParquetSource, sources::Source};


pub trait Pipeline {
    fn run(&self) -> PolarsResult<()>
    {
        Ok(())
    }
}

#[derive(Clone)]
pub struct SimplePipeline {
    source: ParquetSource,
    sink: Sinker,
}

impl SimplePipeline {
    pub fn new(source: ParquetSource, sink: Sinker) -> Self {
        Self { source, sink }
    }
}

impl Pipeline for SimplePipeline {
    fn run(&self) -> PolarsResult<()> {
        let df = self.source.load_data()?;
        self.sink.save_data(&df)?;
        Ok(())
    }
}