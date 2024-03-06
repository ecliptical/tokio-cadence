# Metric Sinks for Cadence built on Tokio

[![Crates.io](https://img.shields.io/crates/v/tokio-cadence.svg)](https://crates.io/crates/tokio-cadence/)
[![Docs.rs](https://docs.rs/tokio-cadence/badge.svg)](https://docs.rs/tokio-cadence/)

A collection of [Cadence](https://crates.io/crates/cadence/) [Metric Sink](https://docs.rs/cadence/0.29/cadence/trait.MetricSink.html)
implementations that process metrics asynchronously using [Tokio](https://crates.io/crates/tokio/).

The Statsd client provided by Cadence does not support asynchronous operation -- submitting a metric may in fact block the caller!

This is undesirable in asynchronous contexts.

The Metric Sinks implemented in this crate alleviate this issue by allowing the application code to enqueue
metrics without blocking, and offloading their actual sending to a separate asynchronous task.

## Features

- emit metrics without blocking by utilizing a non-blocking, buffered channel
- batch multiple metrics, up to a maximum delay (or when the batch buffer fills up)
- process metrics asynchronously in a Tokio task pool of the application's choice

## Installation

Add `cadence`, `tokio`, and `tokio-cadence` to your `Cargo.toml`:

```toml
[dependencies]
cadence = "1"
tokio = { version = "1", features = ["full"] }
tokio-cadence = "0.5"
```

## Usage

The Metric Sink constructors return a tuple consisting of the sink instance itself as well as a future,
which the application must drive to completion; e.g., spawn it in a Tokio task pool.

```rust
use cadence::prelude::*;
use cadence::{StatsdClient, DEFAULT_PORT};
use tokio_cadence::TokioBatchUdpMetricSink;
use tokio::{spawn, net::UdpSocket};

#[tokio::main]
async fn main() -> cadence::MetricResult<()> {
    let host = ("metrics.example.com", DEFAULT_PORT);
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    let (sink, process) = TokioBatchUdpMetricSink::from(host, socket)?;

    // Spawn the future!
    let processing_job = spawn(process);

    {
        let client = StatsdClient::from_sink("my.metrics", sink);

        // Emit metrics!
        client.incr("some.counter");
        client.time("some.methodCall", 42);
        client.gauge("some.thing", 7);
        client.meter("some.value", 5);

        // the client drops here, and the sink along with it
    }

    // Wait for the processing job to complete!
    processing_job.await.unwrap();
    Ok(())
}
```

Note that in order to ensure that all buffered metrics are submitted, the application must await
the completion of the processing job *after* the client, as well as the sink along with it, are dropped.

## License

Licensed under the [MIT license](LICENSE).
