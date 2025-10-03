use polars::{error::PolarsResult, frame::DataFrame};
use crate::{pipelines::Pipeline, sinks::{Sink, Sinker}, sources::{ Source, SourceKind}};




#[derive(Clone)]
pub struct SimplePipeline {
    source: SourceKind,
    sink: Sinker,
}

impl SimplePipeline {
    pub fn new(source: SourceKind, sink: Sinker) -> Self {
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
