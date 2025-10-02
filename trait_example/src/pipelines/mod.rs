use polars::error::PolarsResult;

pub mod simple;
pub trait Pipeline {
    fn name(&self) -> String{
        "simple".to_string()
    }
    
    fn run(&self) -> PolarsResult<()>
    {
        Ok(())
    }
}