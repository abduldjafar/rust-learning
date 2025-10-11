use tracing::{info, instrument};
use polars::frame::DataFrame;
use crate::errors::Result;
use crate::{pipelines::Pipeline, sinks::Sinker, sources::SourceKind};

type Operation<'a> = Box<dyn Fn(&mut DataFrame) -> Result<DataFrame> + Send + Sync + 'a>;

pub struct Job<'a> {
    name: &'a str,
    source: SourceKind<'a>,
    sink: Sinker<'a>,
    operations: Vec<Operation<'a>>,
}

impl<'a> Job<'a> {
    pub fn new(name: &'a str, source: SourceKind<'a>, sink: Sinker<'a>) -> Self {
        Self {
            name,
            source,
            sink,
            operations: Vec::new(),
        }
    }
    
    pub fn with_operation<F>(mut self, operation: F) -> Self
    where
        F: Fn(&mut DataFrame) -> Result<DataFrame> + Send + Sync + 'a,
    {
        self.operations.push(Box::new(operation));
        self
    }

    #[instrument(skip(self), fields(job_name = %self.name))]
    pub async fn run(&self) -> Result<()> {
        info!("Running job: {} with {} operations", self.name, self.operations.len());
        
        let mut pipeline_builder = Pipeline::builder()
            .source(self.source.clone())
            .sink(self.sink.clone());
        
        // Add all operations
        for (i, operation) in self.operations.iter().enumerate() {
            let idx = i;
            pipeline_builder = pipeline_builder.operation(move |df| {
                info!("Executing operation {}/{}", idx + 1, self.operations.len());
                operation(df)
            });
        }
        
        let pipeline = pipeline_builder.build()?;
        pipeline.run().await?;
        
        info!("Job {} completed successfully", self.name);
        Ok(())
    }
}