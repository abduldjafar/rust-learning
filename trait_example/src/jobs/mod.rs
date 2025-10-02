use polars::error::PolarsResult;

use crate::pipelines::Pipeline;

pub mod simple;
pub trait Job {
    fn get_name(&self) -> String;

    fn get_schedule(&self) -> String;

    fn set_name(&self, name: String);

    fn set_schedule(&self, schedule: String);

    fn set_pipeline(&self) -> PolarsResult<()> {
        Ok(())
    }
    fn start(&self) -> PolarsResult<()> {
        Ok(())
    }
    fn stop(&self) -> PolarsResult<()> {
        Ok(())
    }
    fn get_status(&self) -> PolarsResult<()> {
        Ok(())
    }
}