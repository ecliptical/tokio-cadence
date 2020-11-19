//! TODO docs!

use tokio::time::Duration;

use crate::{
    DEFAULT_BATCH_BUF_SIZE,
    DEFAULT_MAX_BATCH_DELAY,
    DEFAULT_QUEUE_CAPACITY,
};

/// TODO docs!
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

    /// TODO docs!
    pub fn queue_cap(&mut self, queue_cap: usize) -> &mut Self {
        self.queue_cap = queue_cap;
        self
    }

    /// TODO docs!
    pub fn buf_size(&mut self, buf_size: usize) -> &mut Self {
        self.buf_size = buf_size;
        self
    }

    /// TODO docs!
    pub fn max_delay(&mut self, max_delay: Duration) -> &mut Self {
        self.max_delay = max_delay;
        self
    }
}
