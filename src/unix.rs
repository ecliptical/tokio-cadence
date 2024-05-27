#![allow(clippy::type_complexity)]

//! Asynchronous Metric Sink implementation that uses Unix Datagram sockets.

use cadence::{ErrorKind as MetricErrorKind, MetricError, MetricResult, MetricSink};

use std::{
    io::Result,
    panic::{RefUnwindSafe, UnwindSafe},
    path::Path,
};

use tokio::{
    net::UnixDatagram,
    sync::mpsc::{channel, Sender},
};

use crate::{
    builder::Builder,
    define_worker,
    worker::{Cmd, TrySend},
    MetricFuture,
};

impl<T: AsRef<Path> + Send + Sync + Unpin + 'static> Builder<T, UnixDatagram> {
    /// Creates a customized instance of the [`TokioBatchUnixMetricSink`](crate::unix::TokioBatchUnixMetricSink).
    ///
    /// # Errors
    ///
    /// Returns an error when the configured queue capacity is 0.
    pub fn build(self) -> MetricResult<(TokioBatchUnixMetricSink, MetricFuture)> {
        if self.queue_cap == 0 {
            return Err(MetricError::from((
                MetricErrorKind::InvalidInput,
                "Queue capacity must be greater than 0",
            )));
        }

        let (tx, rx) = channel(self.queue_cap);
        let worker_fut = worker(rx, self.sock, self.addr, self.buf_size, self.max_delay);

        Ok((TokioBatchUnixMetricSink { tx }, Box::pin(worker_fut)))
    }
}

/// Metric sink that allows clients to enqueue metrics without blocking, and sending
/// them asynchronously over a [Unix Domain Socket](https://docs.datadoghq.com/developers/dogstatsd/unix_socket)
/// using Tokio runtime.
///
/// It also accumulates individual metrics for a configured maximum amount of time
/// before submitting them as a single [batch](https://github.com/statsd/statsd/blob/master/docs/metric_types.md#multi-metric-packets).
///
/// Exceeding the configured queue capacity results in an error, which the client may handle as appropriate.
///
/// ## Important!
/// The client is responsible for polling the asynchronous processing future, which is created along
/// with the sink, in a manner appropriate for the application (e.g., spawning it in a Tokio task pool).
///
/// The client should also wait for this future to complete *after* dropping the metric sink.
///
/// ### Example
///
/// ```no_run
/// use cadence::prelude::*;
/// use cadence::StatsdClient;
/// use tokio_cadence::TokioBatchUnixMetricSink;
/// use tokio::{spawn, net::UnixDatagram};
///
/// # #[tokio::main]
/// # async fn main() -> cadence::MetricResult<()> {
/// let path = "/var/run/datadog/dsd.socket";
/// let socket = UnixDatagram::unbound()?;
/// let (sink, process) = TokioBatchUnixMetricSink::from(path, socket)?;
///
/// // Spawn the future!
/// let processing_job = spawn(process);
///
/// {
///     let client = StatsdClient::from_sink("my.metrics", sink);
///
///     // Emit metrics!
///     client.incr("some.counter");
///     client.time("some.methodCall", 42);
///     client.gauge("some.thing", 7);
///     client.meter("some.value", 5);
///
///     // the client drops here, and the sink along with it
/// }
///
/// // Wait for the processing job to complete!
/// processing_job.await.unwrap();
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct TokioBatchUnixMetricSink {
    tx: Sender<Cmd>,
}

// we don't let tx panic
impl UnwindSafe for TokioBatchUnixMetricSink {}
impl RefUnwindSafe for TokioBatchUnixMetricSink {}

impl TokioBatchUnixMetricSink {
    /// Creates a new metric sink for the given statsd socket path using an unbound Unix socket.
    /// Other sink parameters are defaulted.
    ///
    /// # Errors
    ///
    /// Returns an error
    pub fn from<T: AsRef<Path> + Send + Sync + Unpin + 'static>(
        path: T,
        socket: UnixDatagram,
    ) -> MetricResult<(Self, MetricFuture)> {
        Self::builder(path, socket).build()
    }

    /// Returns a builder for creating a new metric sink for the given statsd socket path
    /// using an unbound Unix socket. The builder may be used to customize various
    /// configuration parameters before creating an instance of this sink.
    pub fn builder<T: AsRef<Path> + Send + Sync + Unpin + 'static>(
        path: T,
        socket: UnixDatagram,
    ) -> Builder<T, UnixDatagram> {
        Builder::new(path, socket)
    }
}

impl TrySend for TokioBatchUnixMetricSink {
    fn sender(&self) -> &Sender<Cmd> {
        &self.tx
    }
}

impl MetricSink for TokioBatchUnixMetricSink {
    fn emit(&self, metric: &str) -> Result<usize> {
        self.try_send(Cmd::Write(metric.to_string()))?;
        Ok(metric.len())
    }

    fn flush(&self) -> Result<()> {
        self.try_send(Cmd::Flush)?;
        Ok(())
    }
}

define_worker!(
    UnixDatagram,
    impl AsRef<Path> + Unpin,
    impl AsRef<Path> + Unpin
);

#[cfg(test)]
mod tests {
    use super::*;
    use log::debug;
    use std::{env::temp_dir, time::UNIX_EPOCH};
    use tokio::{net::UnixDatagram, spawn};

    #[tokio::test]
    async fn from() -> MetricResult<()> {
        let socket = UnixDatagram::unbound()?;
        let path = temp_dir().join(format!(
            "tokio-cadence-test-{}.sock",
            UNIX_EPOCH.elapsed().unwrap_or_default().as_millis()
        ));
        let result = TokioBatchUnixMetricSink::from(path, socket);

        assert!(result.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn emit() -> MetricResult<()> {
        pretty_env_logger::try_init().ok();

        let path = temp_dir().join(format!(
            "test_emit-{}.sock",
            UNIX_EPOCH.elapsed().unwrap_or_default().as_millis()
        ));
        let server_socket = UnixDatagram::bind(&path)?;

        let socket = UnixDatagram::unbound()?;

        let (sink, fut) = TokioBatchUnixMetricSink::from(path, socket)?;

        let worker = spawn(fut);

        const MSG: &str = "test";
        let n = sink.emit(MSG)?;
        assert_eq!(MSG.len(), n);

        let mut buf = [0; 8192];
        let (received, addr) = server_socket.recv_from(&mut buf).await?;

        debug!(
            "received {} bytes from {:?} with {}",
            received,
            addr,
            String::from_utf8_lossy(&buf[..received])
        );

        assert_eq!(MSG.len(), received);
        assert_eq!(MSG, String::from_utf8_lossy(&buf[..received]));

        drop(sink);
        worker.await.unwrap();
        Ok(())
    }

    #[tokio::test]
    async fn emit_multi() -> MetricResult<()> {
        pretty_env_logger::try_init().ok();

        let path = temp_dir().join(format!(
            "test_emit_multi-{}.sock",
            UNIX_EPOCH.elapsed().unwrap_or_default().as_millis()
        ));
        let server_socket = UnixDatagram::bind(&path)?;

        let socket = UnixDatagram::unbound()?;

        const BUF_SIZE: usize = 10;
        let mut builder = TokioBatchUnixMetricSink::builder(path, socket);
        builder.buf_size(BUF_SIZE);
        let (sink, fut) = builder.build()?;

        let worker = spawn(fut);

        const MSG: &str = "test_multi";
        let n = sink.emit(MSG)?;
        assert_eq!(BUF_SIZE, n);
        let n = sink.emit(MSG)?;
        assert_eq!(BUF_SIZE, n);

        let mut buf = [0; 8192];
        let (received, addr) = server_socket.recv_from(&mut buf).await?;

        debug!(
            "received {} bytes from {:?} with {}",
            received,
            addr,
            String::from_utf8_lossy(&buf[..received])
        );

        assert_eq!(MSG.len(), received);
        assert_eq!(MSG, String::from_utf8_lossy(&buf[..received]));

        let (received, addr) = server_socket.recv_from(&mut buf).await?;

        debug!(
            "received {} bytes from {:?} with {}",
            received,
            addr,
            String::from_utf8_lossy(&buf[..received])
        );

        assert_eq!(MSG.len(), received);
        assert_eq!(MSG, String::from_utf8_lossy(&buf[..received]));

        drop(sink);
        worker.await.unwrap();
        Ok(())
    }
}
