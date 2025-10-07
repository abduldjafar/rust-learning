use crate::{pipelines::Pipeline, sinks::Sinker, sources::SourceKind};

pub struct Job<'a> {
    name: String,
    source: SourceKind<'a>,
    sink: Sinker<'a>,
}

impl<'a> Job<'a> {
    pub fn new(name: String, source: SourceKind<'a>, sink: Sinker<'a>) -> Self {
        Self { name, source, sink }
    }

    pub fn run(&self) -> polars::prelude::PolarsResult<()> {
        println!("Running job: {}", self.name);
        let pipeline = Pipeline::new(self.source.clone(), self.sink.clone());
        pipeline.run()
    }
}
