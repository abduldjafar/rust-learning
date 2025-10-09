use tracing::info;

use crate::errors::Result;
use crate::{pipelines::Pipeline, sinks::Sinker, sources::SourceKind};

pub struct Job<'a> {
    name: &'a str,
    source: SourceKind<'a>,
    sink: Sinker<'a>,
}

impl<'a> Job<'a> {
    pub fn new(name: &'a str, source: SourceKind<'a>, sink: Sinker<'a>) -> Self {
        Self { name, source, sink }
    }

    pub async fn run(&self) -> Result<()> {
        info!("Running job: {}", self.name);
        let pipeline = Pipeline::new(self.source.clone(), self.sink.clone());
        pipeline.run().await
    }
}
