use cadence::{
    ErrorKind,
    MetricError,
    MetricResult,
    MetricSink,
};

use std::{
    io::Result,
    net::{
        SocketAddr,
        ToSocketAddrs,
    },
};

use tokio::net::UdpSocket;

pub struct TokioUdpMetricSink {
    addr: SocketAddr,
    socket: UdpSocket,
}

impl TokioUdpMetricSink {
    pub fn from<T: ToSocketAddrs>(host: T, socket: UdpSocket) -> MetricResult<Self> {
        let mut addrs = host.to_socket_addrs()?;
        let addr = addrs.next().ok_or_else(|| {
            MetricError::from((ErrorKind::InvalidInput, "No socket addresses yielded"))
        })?;

        Ok(Self { addr, socket })
    }
}

impl MetricSink for TokioUdpMetricSink {
    fn emit(&self, metric: &str) -> Result<usize> {
        self.socket.try_send_to(metric.as_bytes(), self.addr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Result;
    use tokio::net::UdpSocket;

    #[tokio::test]
    async fn from() -> Result<()> {
        let socket = UdpSocket::bind("0.0.0.0:0").await?;
        let sink = TokioUdpMetricSink::from("127.0.0.1:65534", socket);

        assert!(sink.is_ok());

        Ok(())
    }
}
