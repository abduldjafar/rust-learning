use std::pin::Pin;
use std::task::{Context, Poll};
use futures::{Stream, StreamExt};
use tokio::time::{sleep, Duration};
use std::future::Future;

struct Counter {
    count: usize,
    max: usize,
    delay: Option<Pin<Box<dyn Future<Output = ()> + Send>>>, // Holds the async delay
}

impl Counter {
    fn new(max: usize) -> Self {
        Self { 
            count: 0, 
            max, 
            delay: None, 
        }
    }
}

impl Stream for Counter {
    type Item = usize;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let self_mut = self.get_mut();

        // If we have a delay in progress, poll it first
        if let Some(ref mut delay) = self_mut.delay {
            // Poll the delay future
            match delay.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending, // Still waiting
                Poll::Ready(_) => self_mut.delay = None, // Delay complete
            }
        }

        // Produce the next value if the count is less than max
        if self_mut.count < self_mut.max {
            self_mut.count += 1;

            // Start a new delay for the next item
            self_mut.delay = Some(Box::pin(sleep(Duration::from_millis(500))));

            Poll::Ready(Some(self_mut.count))
        } else {
            Poll::Ready(None) // End of the stream
        }
    }
}

#[tokio::main]
pub async fn run() {
    let mut stream = Counter::new(5);

    while let Some(value) = stream.next().await {
        println!("Got value: {}", value);
    }
}
