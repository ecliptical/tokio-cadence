//! Builders for customizing asynchronous Metric Sinks.

use tokio::time::Duration;

use crate::{DEFAULT_BATCH_BUF_SIZE, DEFAULT_MAX_BATCH_DELAY, DEFAULT_QUEUE_CAPACITY};

/// Builder allows you to override various default parameter values before creating an instance
/// of the desired Metric Sink.
#[derive(Debug)]
pub struct Builder<T, S> {
    pub(crate) addr: T,
    pub(crate) sock: S,
    pub(crate) queue_cap: usize,
    pub(crate) buf_size: usize,
    pub(crate) max_delay: Duration,
}

impl<T, S> Builder<T, S> {
    pub(crate) fn new(addr: T, sock: S) -> Self {
        Self {
            addr,
            sock,
            queue_cap: DEFAULT_QUEUE_CAPACITY,
            buf_size: DEFAULT_BATCH_BUF_SIZE,
            max_delay: DEFAULT_MAX_BATCH_DELAY,
        }
    }

    /// Sets the maximum metric queue capacity (default: [DEFAULT_QUEUE_CAPACITY](crate::DEFAULT_QUEUE_CAPACITY)).
    pub fn queue_cap(&mut self, queue_cap: usize) -> &mut Self {
        self.queue_cap = queue_cap;
        self
    }

    /// Sets the batch buffer size (default: [DEFAULT_BATCH_BUF_SIZE](crate::DEFAULT_BATCH_BUF_SIZE)).
    pub fn buf_size(&mut self, buf_size: usize) -> &mut Self {
        self.buf_size = buf_size;
        self
    }

    /// Sets the maximum delay before flushing any buffered metrics (default: [DEFAULT_MAX_BATCH_DELAY](crate::DEFAULT_MAX_BATCH_DELAY)).
    pub fn max_delay(&mut self, max_delay: Duration) -> &mut Self {
        self.max_delay = max_delay;
        self
    }
}
