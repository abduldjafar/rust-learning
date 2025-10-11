use polars::frame::DataFrame;
use crate::errors::{Error, Result};
use crate::{
    sinks::{Sink, Sinker},
    sources::{Source, SourceKind},
};

pub struct PipelineBuilder<'a> {
    source: Option<SourceKind<'a>>,
    sink: Option<Sinker<'a>>,
    operations: Vec<Box<dyn Fn(&mut DataFrame) -> Result<DataFrame> + Send + Sync + 'a>>,
}

impl<'a> PipelineBuilder<'a> {
    pub fn new() -> Self {
        Self {
            source: None,
            sink: None,
            operations: Vec::new(),
        }
    }
    
    pub fn source(mut self, source: SourceKind<'a>) -> Self {
        self.source = Some(source);
        self
    }
    
    pub fn sink(mut self, sink: Sinker<'a>) -> Self {
        self.sink = Some(sink);
        self
    }
    
    pub fn operation<F>(mut self, op: F) -> Self
    where
        F: Fn(&mut DataFrame) -> Result<DataFrame> + Send + Sync + 'a,
    {
        self.operations.push(Box::new(op));
        self
    }
    
    pub fn build(self) -> Result<Pipeline<'a>> {
        Ok(Pipeline {
            source: self.source.ok_or_else(|| Error::Polars("Source required".to_string()))?,
            sink: self.sink.ok_or_else(|| Error::Polars("Source required".to_string()))?,
            operations: self.operations,
        })
    }
}

pub struct Pipeline<'a> {
    source: SourceKind<'a>,
    sink: Sinker<'a>,
    operations: Vec<Box<dyn Fn(&mut DataFrame) -> Result<DataFrame> + Send + Sync + 'a>>,
}

impl<'a> Pipeline<'a> {
    pub fn builder() -> PipelineBuilder<'a> {
        PipelineBuilder::new()
    }

    pub async fn run(&self) -> Result<()> {
        let mut df = self.source.load_data().await?;
        
        for (i, operation) in self.operations.iter().enumerate() {
            tracing::debug!("Applying operation {}", i + 1);
            df = operation(&mut df)?;
        }
        
        self.sink.save_data(&mut df).await?;
        Ok(())
    }
}