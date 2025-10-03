use crate::jobs::{Job, SimpleJob};

impl Job for SimpleJob {
    fn run(&self) -> polars::prelude::PolarsResult<()> {
        self.start()
    }
}
