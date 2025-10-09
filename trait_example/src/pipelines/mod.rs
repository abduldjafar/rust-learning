use crate::errors::Result;

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

    pub async fn run(&self) -> Result<()> {
        let mut df = self.source.load_data().await?;
        self.sink.save_data(&mut df).await?;
        Ok(())
    }
}
