use cadence::{
    ErrorKind as MetricErrorKind,
    MetricError,
    MetricResult,
    MetricSink,
};

use log::*;
use std::{
    future::Future,
    io::{
        Error,
        ErrorKind,
        Result,
    },
    net::{
        SocketAddr,
        ToSocketAddrs,
    },
    pin::Pin,
};

use tokio::{
    net::UdpSocket,
    sync::mpsc::{
        channel,
        error::TrySendError,
        Receiver,
        Sender,
    },
    time::{
        timeout_at,
        Duration,
        Instant,
    },
};

pub const DEFAULT_QUEUE_LEN: usize = 1024;
pub const DEFAULT_BUF_SIZE: usize = 1432;
pub const DEFAULT_MAX_DELAY_MS: u64 = 1000;

#[derive(Debug)]
pub struct TokioBatchUdpMetricSink {
    tx: Sender<Cmd>,
}

impl TokioBatchUdpMetricSink {
    pub fn from<T: ToSocketAddrs>(
        host: T,
        socket: UdpSocket,
    ) -> MetricResult<(
        Self,
        Pin<Box<dyn Future<Output = ()> + Send + Sync + 'static>>,
    )> {
        Self::with_capacity(
            host,
            socket,
            DEFAULT_QUEUE_LEN,
            DEFAULT_BUF_SIZE,
            DEFAULT_MAX_DELAY_MS,
        )
    }

    pub fn with_capacity<T: ToSocketAddrs>(
        host: T,
        socket: UdpSocket,
        queue_len: usize,
        buf_size: usize,
        max_delay: u64,
    ) -> MetricResult<(
        Self,
        Pin<Box<dyn Future<Output = ()> + Send + Sync + 'static>>,
    )> {
        let mut addrs = host.to_socket_addrs()?;
        let addr = addrs.next().ok_or_else(|| {
            MetricError::from((MetricErrorKind::InvalidInput, "No socket addresses yielded"))
        })?;

        let (tx, rx) = channel(queue_len);
        let worker_fut = worker(rx, addr, socket, buf_size, max_delay);

        Ok((Self { tx }, Box::pin(worker_fut)))
    }

    fn try_send(&self, cmd: Cmd) -> Result<()> {
        let mut tx = self.tx.clone();
        if let Err(e) = tx.try_send(cmd) {
            let kind = match e {
                TrySendError::Full(_) => ErrorKind::WouldBlock,
                TrySendError::Closed(_) => ErrorKind::Other,
            };

            return Err(Error::new(kind, e));
        }

        Ok(())
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

#[derive(Debug)]
enum Cmd {
    Write(String),
    Flush,
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
    max_delay: u64,
) {
    let mut buf = String::with_capacity(buf_size);
    let mut deadline = Instant::now() + Duration::from_millis(max_delay);
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
                            deadline = Instant::now() + Duration::from_millis(max_delay);
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

                deadline = Instant::now() + Duration::from_millis(max_delay);
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

                deadline = Instant::now() + Duration::from_millis(max_delay);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{
        Error,
        ErrorKind,
        Result,
    };

    use tokio::{
        net::UdpSocket,
        spawn,
    };

    #[tokio::test]
    async fn from() -> Result<()> {
        let socket = UdpSocket::bind("0.0.0.0:0").await?;
        let result = TokioBatchUdpMetricSink::from("127.0.0.1:8125", socket);

        assert!(result.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn from_bad_address() -> Result<()> {
        let socket = UdpSocket::bind("0.0.0.0:0").await?;
        let result = TokioBatchUdpMetricSink::from("bad address", socket);

        assert!(result.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn emit() -> Result<()> {
        pretty_env_logger::try_init().ok();

        let mut server_socket = UdpSocket::bind("127.0.0.1:0").await?;
        let server_addr = server_socket.local_addr()?;

        debug!("server socket: {}", server_addr);

        let socket = UdpSocket::bind("0.0.0.0:0").await?;

        debug!("local socket: {}", socket.local_addr()?);

        let (sink, fut) =
            TokioBatchUdpMetricSink::from(format!("127.0.0.1:{}", server_addr.port()), socket)
                .map_err(|e| Error::new(ErrorKind::Other, e))?;

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
        worker.await?;
        Ok(())
    }

    #[tokio::test]
    async fn emit_multi() -> Result<()> {
        pretty_env_logger::try_init().ok();

        let mut server_socket = UdpSocket::bind("127.0.0.1:0").await?;
        let server_addr = server_socket.local_addr()?;

        debug!("server socket: {}", server_addr);

        let socket = UdpSocket::bind("0.0.0.0:0").await?;

        debug!("local socket: {}", socket.local_addr()?);

        const BUF_SIZE: usize = 10;
        let (sink, fut) = TokioBatchUdpMetricSink::with_capacity(
            format!("127.0.0.1:{}", server_addr.port()),
            socket,
            DEFAULT_QUEUE_LEN,
            BUF_SIZE,
            DEFAULT_MAX_DELAY_MS,
        )
        .map_err(|e| Error::new(ErrorKind::Other, e))?;

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
        worker.await?;
        Ok(())
    }
}
