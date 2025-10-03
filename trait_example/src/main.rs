use trait_example::jobs::{Job, SimpleJob};

fn main() {
    let job = SimpleJob::set("simple".to_string(), "0 * * * *".to_string());
    job.run().unwrap();
}
