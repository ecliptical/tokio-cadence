//! TODO docs!

use cadence::{
    MetricResult,
    MetricSink,
};

use log::*;
use std::{
    future::Future,
    io::Result,
    panic::{
        RefUnwindSafe,
        UnwindSafe,
    },
    path::Path,
    pin::Pin,
};

use tokio::{
    net::UnixDatagram,
    sync::mpsc::{
        channel,
        Receiver,
        Sender,
    },
    time::{
        timeout_at,
        Duration,
        Instant,
    },
};

use crate::{
    builder::Builder,
    worker::{
        Cmd,
        TrySend,
    },
};

impl<T: AsRef<Path> + Send + Sync + Unpin + 'static> Builder<T, UnixDatagram> {
    /// TODO docs!
    pub fn build(
        self,
    ) -> MetricResult<(
        TokioBatchUnixMetricSink,
        Pin<Box<dyn Future<Output = ()> + Send + Sync + 'static>>,
    )> {
        let (tx, rx) = channel(self.queue_cap);
        let worker_fut = worker(rx, self.addr, self.sock, self.buf_size, self.max_delay);

        Ok((TokioBatchUnixMetricSink { tx }, Box::pin(worker_fut)))
    }
}

/// Metric sink that allows clients to enqueue metrics without blocking, and processing
/// them asynchronously over a Unix Domain Socket using Tokio runtime.
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
    /// Creates a new metric sink for the given statsd host using a previously bound Unix socket.
    /// Other sink parameters are defaulted.
    pub fn from<T: AsRef<Path> + Send + Sync + Unpin + 'static>(
        host: T,
        socket: UnixDatagram,
    ) -> MetricResult<(
        Self,
        Pin<Box<dyn Future<Output = ()> + Send + Sync + 'static>>,
    )> {
        Self::builder(host, socket).build()
    }

    /// TODO docs!
    pub fn builder<T: AsRef<Path> + Send + Sync + Unpin + 'static>(
        path: T,
        sock: UnixDatagram,
    ) -> Builder<T, UnixDatagram> {
        Builder::new(path, sock)
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

async fn do_send<T: AsRef<Path> + Unpin>(socket: &mut UnixDatagram, addr: &T, buf: &mut String) {
    match socket.send_to(buf.as_bytes(), addr).await {
        Ok(n) => {
            debug!("sent {} bytes", n);
        }

        Err(e) => {
            error!("failed to send metrics: {:?}", e);
        }
    }

    buf.clear();
}

async fn worker(
    mut rx: Receiver<Cmd>,
    addr: impl AsRef<Path> + Unpin,
    mut socket: UnixDatagram,
    buf_size: usize,
    max_delay: Duration,
) {
    let mut buf = String::with_capacity(buf_size);
    let now = Instant::now();
    let mut deadline = now.checked_add(max_delay).unwrap_or(now);
    loop {
        match timeout_at(deadline, rx.recv()).await {
            Ok(Some(Cmd::Write(msg))) => {
                trace!("write: {}", msg);

                let msg_len = msg.len();
                if msg_len > buf.capacity() {
                    warn!("metric exceeds buffer capacity: {}", msg);
                } else {
                    let buf_len = buf.len();
                    if buf_len > 0 {
                        if buf_len + 1 + msg_len > buf.capacity() {
                            do_send(&mut socket, &addr, &mut buf).await;
                            let now = Instant::now();
                            deadline = now.checked_add(max_delay).unwrap_or(now);
                        } else {
                            buf.push('\n');
                        }
                    }

                    buf.push_str(&msg);
                }
            }

            Ok(Some(Cmd::Flush)) => {
                trace!("flush");

                if !buf.is_empty() {
                    do_send(&mut socket, &addr, &mut buf).await;
                }

                let now = Instant::now();
                deadline = now.checked_add(max_delay).unwrap_or(now);
            }

            Ok(None) => {
                debug!("stop");

                if !buf.is_empty() {
                    do_send(&mut socket, &addr, &mut buf).await;
                }

                break;
            }

            Err(_) => {
                trace!("timeout");

                if !buf.is_empty() {
                    do_send(&mut socket, &addr, &mut buf).await;
                }

                let now = Instant::now();
                deadline = now.checked_add(max_delay).unwrap_or(now);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        env::temp_dir,
        time::UNIX_EPOCH,
    };
    use tokio::{
        net::UnixDatagram,
        spawn,
    };

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
        let mut server_socket = UnixDatagram::bind(&path)?;

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
        let mut server_socket = UnixDatagram::bind(&path)?;

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
