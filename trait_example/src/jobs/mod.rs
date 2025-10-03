mod simple;
mod execution;
pub trait Job {
    fn run(&self) -> polars::prelude::PolarsResult<()>;
}

pub struct SimpleJob {
    pub name: String,
    pub schedule: String,
    pub status: String,
}


