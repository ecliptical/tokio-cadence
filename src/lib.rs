#![deny(missing_docs)]
#![deny(clippy::pedantic)]

//! A collection of Cadence [Metric Sink](https://docs.rs/cadence/0.22/cadence/trait.MetricSink.html)
//! implementations that operate asynchronously using [Tokio](https://docs.rs/tokio/0.2/tokio).

use std::{future::Future, pin::Pin};
use tokio::time::Duration;

pub mod builder;
pub mod udp;
pub mod unix;
mod worker;

pub use builder::Builder;
pub use udp::TokioBatchUdpMetricSink;
pub use unix::TokioBatchUnixMetricSink;

/// Default length of metrics queue.
pub const DEFAULT_QUEUE_CAPACITY: usize = 1024;

/// Default size of buffer used for batching metrics.
pub const DEFAULT_BATCH_BUF_SIZE: usize = 1432;

/// Default maximum delay to wait before submitting accumulated metrics as a single batch,
/// in milliseconds.
#[deprecated = "please use `DEFAULT_MAX_BATCH_DELAY` instead"]
pub const DEFAULT_MAX_BATCH_DELAY_MS: u64 = 1000;

/// Default maximum delay to wait before submitting accumulated metrics as a single batch.
#[allow(deprecated)]
pub const DEFAULT_MAX_BATCH_DELAY: Duration = Duration::from_millis(DEFAULT_MAX_BATCH_DELAY_MS);

pub(crate) type MetricFuture = Pin<Box<dyn Future<Output = ()> + Send + Sync + 'static>>;
