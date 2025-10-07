use polars::error::PolarsResult;

use crate::{
    sinks::{Sink, Sinker},
    sources::{Source, SourceKind},
};

#[derive(Clone)]
pub struct Pipeline<'a> {
    source: SourceKind<'a>,
    sink: Sinker<'a>,
}

impl<'a> Pipeline<'a> {
    pub fn new(source: SourceKind<'a>, sink: Sinker<'a>) -> Self {
        Self { source, sink }
    }

    pub fn run(&self) -> PolarsResult<()> {
        let df = self.source.load_data()?;
        self.sink.save_data(&df)?;
        Ok(())
    }
}
