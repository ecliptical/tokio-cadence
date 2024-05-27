use std::{
    io::{Error, ErrorKind, Result},
    panic::{catch_unwind, RefUnwindSafe, UnwindSafe},
    process::abort,
};

use tokio::sync::mpsc::{error::TrySendError, Sender};

#[derive(Clone, Debug)]
pub enum Cmd {
    Write(String),
    Flush,
}

pub trait TrySend: UnwindSafe + RefUnwindSafe {
    fn sender(&self) -> &Sender<Cmd>;

    fn try_send(&self, cmd: Cmd) -> Result<()> {
        // self.tx is !RefUnwindSafe -- don't let it panic!
        let wrapped = catch_unwind(|| {
            let tx = self.sender().clone();
            tx.try_send(cmd)
        });

        match wrapped {
            Ok(res) => {
                if let Err(e) = res {
                    let kind = match e {
                        TrySendError::Full(_) => ErrorKind::WouldBlock,
                        TrySendError::Closed(_) => ErrorKind::Other,
                    };

                    return Err(Error::new(kind, e));
                }
            }

            Err(e) => {
                eprintln!("panic while attempting to enqueue statsd metric: {e:?}");
                abort();
            }
        }

        Ok(())
    }
}

// Yes, really! :-(
// I can't figure out how to genericize these functions without boxing futures or copying bytes...
#[doc(hidden)]
#[allow(clippy::module_name_repetitions)]
#[macro_export]
#[allow(clippy::module_name_repetitions)]
macro_rules! define_worker {
    ($SocketType:ty, $TargetType:ty) => {
        $crate::define_worker!($SocketType, $TargetType, &$TargetType);
    };

    ($SocketType:ty, $TargetType:ty, $TargetTypeRef:ty) => {
        async fn do_send(socket: &mut $SocketType, addr: $TargetTypeRef, buf: &mut String) {
            use ::log::{debug, error};

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
            mut rx: ::tokio::sync::mpsc::Receiver<Cmd>,
            mut socket: $SocketType,
            addr: $TargetType,
            buf_size: usize,
            max_delay: ::tokio::time::Duration,
        ) {
            use ::log::{debug, trace, warn};
            use ::tokio::time::{timeout_at, Instant};

            use $crate::worker::Cmd;

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
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::{Error, ErrorKind, Result},
        sync::Arc,
    };

    use tokio::{
        sync::{mpsc, Mutex},
        time::Duration,
    };

    struct TestSocket {
        items: Arc<Mutex<Vec<String>>>,
    }

    impl TestSocket {
        async fn send_to(&mut self, buf: &[u8], _target: &String) -> Result<usize> {
            let mut items = self.items.lock().await;
            items.push(
                String::from_utf8(Vec::from(buf))
                    .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?,
            );

            Ok(buf.len())
        }
    }

    define_worker!(TestSocket, String);

    #[tokio::test]
    async fn handle_single_item() {
        let (tx, rx) = mpsc::channel(1);
        let items = Arc::new(Mutex::new(Vec::default()));
        let socket = TestSocket {
            items: items.clone(),
        };

        tx.send(Cmd::Write("test1".to_string())).await.unwrap();

        drop(tx);

        worker(
            rx,
            socket,
            String::default(),
            10,
            Duration::from_millis(100),
        )
        .await;

        assert_eq!(vec!["test1".to_string()], *items.lock().await);
    }

    #[tokio::test]
    async fn handle_multi_item() {
        let (tx, rx) = mpsc::channel(2);
        let items = Arc::new(Mutex::new(Vec::default()));
        let socket = TestSocket {
            items: items.clone(),
        };

        tx.send(Cmd::Write("test1".to_string())).await.unwrap();
        tx.send(Cmd::Write("test2".to_string())).await.unwrap();

        drop(tx);

        worker(
            rx,
            socket,
            String::default(),
            11,
            Duration::from_millis(100),
        )
        .await;

        assert_eq!(vec!["test1\ntest2".to_string()], *items.lock().await);
    }

    #[tokio::test]
    async fn handle_full_buffer() {
        let (tx, rx) = mpsc::channel(2);
        let items = Arc::new(Mutex::new(Vec::default()));
        let socket = TestSocket {
            items: items.clone(),
        };

        tx.send(Cmd::Write("test1".to_string())).await.unwrap();
        tx.send(Cmd::Write("test2".to_string())).await.unwrap();

        drop(tx);

        worker(
            rx,
            socket,
            String::default(),
            10,
            Duration::from_millis(100),
        )
        .await;

        assert_eq!(
            vec!["test1".to_string(), "test2".to_string()],
            *items.lock().await
        );
    }

    #[tokio::test]
    async fn handle_over_buffer_capacity() {
        let (tx, rx) = mpsc::channel(1);
        let items = Arc::new(Mutex::new(Vec::default()));
        let socket = TestSocket {
            items: items.clone(),
        };

        tx.send(Cmd::Write("test1test2test3".to_string()))
            .await
            .unwrap();

        drop(tx);

        worker(
            rx,
            socket,
            String::default(),
            10,
            Duration::from_millis(100),
        )
        .await;

        assert!(items.lock().await.is_empty());
    }
}
