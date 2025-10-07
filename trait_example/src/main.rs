use trait_example::{jobs::Job, sinks::Sinker, sources::SourceKind};

fn main() {
    let simple_job = Job::new(
        "simple".to_string(),
        SourceKind::read_parquet("parquet"),
        Sinker::csv("output.csv"),
    );
    simple_job.run().unwrap();
}
