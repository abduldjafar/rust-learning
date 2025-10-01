use trait_example::{sinks::{Sink, Sinker}, sources::{ParquetSource, Source}};
use trait_example::pipelines::{Pipeline, SimplePipeline};



fn main() {
    let parquet_source = ParquetSource::new();
    let sink = Sinker::Csv("output.csv".to_string());
    let pipeline = SimplePipeline::new(parquet_source, sink);
    pipeline.run().unwrap();

}
