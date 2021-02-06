use cadence::{prelude::*, StatsdClient};

use log::*;
use std::{
    error::Error,
    io::BufRead,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};

use tokio::{
    net::UdpSocket,
    spawn,
    time::{sleep, Duration},
};

use tokio_cadence::*;

const NUM_TASKS: usize = 100;
const NUM_LOOPS: usize = 1000;
const LOOP_DELAY: Duration = Duration::from_millis(0);
const METRIC_PREFIX: &str = "async_impl";
const COUNT_METRIC: &str = "test";
const FULL_COUNT_METRIC: &str = "async_impl.test";
const CATCHUP_DELAY: Duration = Duration::from_secs(1);

async fn run(client: StatsdClient) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    let mut tasks = Vec::with_capacity(NUM_TASKS);
    let start = Instant::now();

    for _ in 0..NUM_TASKS {
        let client = client.clone();
        let task = spawn(async move {
            for _ in 0..NUM_LOOPS {
                client.incr_with_tags(COUNT_METRIC).send();
                sleep(LOOP_DELAY).await;
            }
        });

        tasks.push(task);
    }

    for task in tasks {
        task.await?;
    }

    info!("elapsed: {}ms", start.elapsed().as_millis());

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    pretty_env_logger::init();

    let client_socket = UdpSocket::bind("127.0.0.1:0").await?;
    let client_addr = client_socket.local_addr()?;

    let server_socket = UdpSocket::bind("127.0.0.1:0").await?;
    let server_addr = server_socket.local_addr()?;

    let recv_count = Arc::new(AtomicUsize::default());
    let recv_count_clone = recv_count.clone();

    spawn(async move {
        let mut buf = [0; 8192];

        while let Ok((n, addr)) = server_socket.recv_from(&mut buf).await {
            if addr == client_addr {
                debug!("data: {}", String::from_utf8_lossy(&buf[..n]));

                &buf[..n]
                    .lines()
                    .filter_map(|line| {
                        line.ok().and_then(|line| {
                            let parts: Vec<_> = line.splitn(2, ':').collect();
                            parts.get(1).and_then(|suffix| {
                                let value_parts: Vec<_> = suffix.splitn(3, '|').take(2).collect();
                                value_parts
                                    .get(1)
                                    .filter(|&suffix| *suffix == "c")
                                    .map(|_| {
                                        (
                                            parts[0].to_string(),
                                            value_parts[0].parse::<usize>().unwrap_or_default(),
                                        )
                                    })
                            })
                        })
                    })
                    .for_each(|(key, value)| {
                        if key == FULL_COUNT_METRIC {
                            recv_count.fetch_add(value, Ordering::AcqRel);
                        }
                    });
            }
        }
    });

    let host = format!("127.0.0.1:{}", server_addr.port());
    let (sink, process) = TokioBatchUdpMetricSink::from(host, client_socket)?;

    let processing_job = spawn(process);

    let error_count = Arc::new(AtomicUsize::default());
    let error_count_clone = error_count.clone();
    let client = StatsdClient::builder(METRIC_PREFIX, sink)
        .with_error_handler(move |_| {
            error_count_clone.fetch_add(1, Ordering::AcqRel);
        })
        .build();

    run(client).await?;

    processing_job.await?;

    sleep(CATCHUP_DELAY).await;

    info!("sent count: {}", NUM_TASKS * NUM_LOOPS);

    info!(
        "received count: {}",
        recv_count_clone.load(Ordering::Acquire)
    );

    info!("error count: {}", error_count.load(Ordering::Acquire));

    Ok(())
}
