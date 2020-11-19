//! TODO docs!

use cadence::{
    ErrorKind as MetricErrorKind,
    MetricError,
    MetricResult,
    MetricSink,
};

use log::*;
use std::{
    future::Future,
    io::Result,
    net::{
        SocketAddr,
        ToSocketAddrs,
    },
    panic::{
        RefUnwindSafe,
        UnwindSafe,
    },
    pin::Pin,
};

use tokio::{
    net::UdpSocket,
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

impl<T: ToSocketAddrs> Builder<T, UdpSocket> {
    /// TODO docs!
    pub fn build(
        self,
    ) -> MetricResult<(
        TokioBatchUdpMetricSink,
        Pin<Box<dyn Future<Output = ()> + Send + Sync + 'static>>,
    )> {
        let mut addrs = self.addr.to_socket_addrs()?;
        let addr = addrs.next().ok_or_else(|| {
            MetricError::from((MetricErrorKind::InvalidInput, "No socket addresses yielded"))
        })?;

        let (tx, rx) = channel(self.queue_cap);
        let worker_fut = worker(rx, addr, self.sock, self.buf_size, self.max_delay);

        Ok((TokioBatchUdpMetricSink { tx }, Box::pin(worker_fut)))
    }
}

/// Metric sink that allows clients to enqueue metrics without blocking, and processing
/// them asynchronously via UDP using Tokio runtime.
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
/// use cadence::{StatsdClient, DEFAULT_PORT};
/// use tokio_cadence::TokioBatchUdpMetricSink;
/// use tokio::{spawn, net::UdpSocket};
///
/// # #[tokio::main]
/// # async fn main() -> cadence::MetricResult<()> {
/// let host = ("metrics.example.com", DEFAULT_PORT);
/// let socket = UdpSocket::bind("0.0.0.0:0").await?;
/// let (sink, process) = TokioBatchUdpMetricSink::from(host, socket)?;
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
pub struct TokioBatchUdpMetricSink {
    tx: Sender<Cmd>,
}

// we don't let tx panic
impl UnwindSafe for TokioBatchUdpMetricSink {}
impl RefUnwindSafe for TokioBatchUdpMetricSink {}

impl TokioBatchUdpMetricSink {
    /// Creates a new metric sink for the given statsd host using a previously bound UDP socket.
    /// Other sink parameters are defaulted.
    pub fn from<T: ToSocketAddrs>(
        host: T,
        socket: UdpSocket,
    ) -> MetricResult<(
        Self,
        Pin<Box<dyn Future<Output = ()> + Send + Sync + 'static>>,
    )> {
        Builder::new(host, socket).build()
    }

    /// Creates a new metric sink for the given statsd host, using the UDP socket, as well as
    /// metric queue capacity, batch buffer size, and maximum delay (in milliseconds) to wait
    /// before submitting any accumulated metrics as a batch.
    #[deprecated = "please use `with_options` instead"]
    pub fn with_capacity<T: ToSocketAddrs>(
        host: T,
        socket: UdpSocket,
        queue_capacity: usize,
        buf_size: usize,
        max_delay: u64,
    ) -> MetricResult<(
        Self,
        Pin<Box<dyn Future<Output = ()> + Send + Sync + 'static>>,
    )> {
        let mut builder = Builder::new(host, socket);
        builder.queue_cap(queue_capacity);
        builder.buf_size(buf_size);
        builder.max_delay(Duration::from_millis(max_delay));
        builder.build()
    }
}

impl TrySend for TokioBatchUdpMetricSink {
    fn sender(&self) -> &Sender<Cmd> {
        &self.tx
    }
}

impl MetricSink for TokioBatchUdpMetricSink {
    fn emit(&self, metric: &str) -> Result<usize> {
        self.try_send(Cmd::Write(metric.to_string()))?;
        Ok(metric.len())
    }

    fn flush(&self) -> Result<()> {
        self.try_send(Cmd::Flush)?;
        Ok(())
    }
}

async fn do_send(socket: &mut UdpSocket, addr: &SocketAddr, buf: &mut String) {
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
    addr: SocketAddr,
    mut socket: UdpSocket,
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
    use tokio::{
        net::UdpSocket,
        spawn,
    };

    #[tokio::test]
    async fn from() -> MetricResult<()> {
        let socket = UdpSocket::bind("0.0.0.0:0").await?;
        let result = TokioBatchUdpMetricSink::from("127.0.0.1:8125", socket);

        assert!(result.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn from_bad_address() -> MetricResult<()> {
        let socket = UdpSocket::bind("0.0.0.0:0").await?;
        let result = TokioBatchUdpMetricSink::from("bad address", socket);

        assert!(result.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn emit() -> MetricResult<()> {
        pretty_env_logger::try_init().ok();

        let mut server_socket = UdpSocket::bind("127.0.0.1:0").await?;
        let server_addr = server_socket.local_addr()?;

        debug!("server socket: {}", server_addr);

        let socket = UdpSocket::bind("0.0.0.0:0").await?;

        debug!("local socket: {}", socket.local_addr()?);

        let (sink, fut) =
            TokioBatchUdpMetricSink::from(format!("127.0.0.1:{}", server_addr.port()), socket)?;

        let worker = spawn(fut);

        const MSG: &str = "test";
        let n = sink.emit(MSG)?;
        assert_eq!(MSG.len(), n);

        let mut buf = [0; 8192];
        let (received, addr) = server_socket.recv_from(&mut buf).await?;

        debug!(
            "received {} bytes from {} with {}",
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

        let mut server_socket = UdpSocket::bind("127.0.0.1:0").await?;
        let server_addr = server_socket.local_addr()?;

        debug!("server socket: {}", server_addr);

        let socket = UdpSocket::bind("0.0.0.0:0").await?;

        debug!("local socket: {}", socket.local_addr()?);

        const BUF_SIZE: usize = 10;
        let mut builder = Builder::new(format!("127.0.0.1:{}", server_addr.port()), socket);
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
            "received {} bytes from {} with {}",
            received,
            addr,
            String::from_utf8_lossy(&buf[..received])
        );

        assert_eq!(MSG.len(), received);
        assert_eq!(MSG, String::from_utf8_lossy(&buf[..received]));

        let (received, addr) = server_socket.recv_from(&mut buf).await?;

        debug!(
            "received {} bytes from {} with {}",
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
