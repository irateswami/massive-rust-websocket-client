//! End-to-end throughput benchmark.
//!
//! Spins up a mock WebSocket server that pushes batches of trade messages
//! at maximum speed, then measures actual messages/sec throughput and tail
//! latency (p99) through the full pipeline:
//!
//!   read_loop → rqueue → process_loop → output_tx
//!
//! Run: `cargo bench --bench throughput`

use std::time::{Duration, Instant};

use criterion::{Criterion, criterion_group, criterion_main};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use massive_rust_client::websocket::{Client, Config, Feed, Market, MarketData, Topic};

const BATCH_SIZE: usize = 50;
const NUM_BATCHES: usize = 200;
const TOTAL_MESSAGES: usize = BATCH_SIZE * NUM_BATCHES;

/// Generates a JSON array of `n` trade messages.
fn make_batch(n: usize, batch_idx: usize) -> String {
    let elements: Vec<String> = (0..n)
        .map(|i| {
            let seq = batch_idx * n + i;
            format!(
                r#"{{"ev":"T","sym":"SPY","p":{},"s":{},"t":{},"q":{}}}"#,
                450.0 + seq as f64 * 0.001,
                100 + (seq % 1000),
                1_700_000_000_000_i64 + seq as i64,
                seq,
            )
        })
        .collect();
    format!("[{}]", elements.join(","))
}

/// Spawns a mock WebSocket server that:
/// 1. Accepts auth
/// 2. Accepts subscribe
/// 3. Immediately blasts `NUM_BATCHES` batches of `BATCH_SIZE` trades
async fn start_blast_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("ws://127.0.0.1:{}", addr.port());

    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let ws = match tokio_tungstenite::accept_async(stream).await {
                    Ok(ws) => ws,
                    Err(_) => return,
                };
                let (mut write, mut read) = ws.split();

                // Wait for auth.
                if let Some(Ok(_)) = read.next().await {
                    let auth_ok = serde_json::json!([{"ev": "status", "status": "auth_success"}]);
                    let _ = write.send(Message::Text(auth_ok.to_string().into())).await;
                }

                // Wait for subscribe.
                if let Some(Ok(_)) = read.next().await {
                    let sub_ok = serde_json::json!([
                        {"ev": "status", "status": "success", "message": "subscribed"}
                    ]);
                    let _ = write.send(Message::Text(sub_ok.to_string().into())).await;

                    // Blast all batches as fast as possible.
                    for batch_idx in 0..NUM_BATCHES {
                        let batch = make_batch(BATCH_SIZE, batch_idx);
                        if write.send(Message::Text(batch.into())).await.is_err() {
                            break;
                        }
                    }
                }

                // Keep alive briefly for draining.
                tokio::time::sleep(Duration::from_secs(5)).await;
            });
        }
    });

    url
}

/// Drains all messages from the output channel, collecting per-message latencies.
async fn drain_output(
    mut output: mpsc::Receiver<MarketData>,
    total: usize,
    start: Instant,
) -> Vec<Duration> {
    let mut latencies = Vec::with_capacity(total);
    let mut count = 0;

    let deadline = start + Duration::from_secs(30);
    loop {
        let timeout = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(timeout, output.recv()).await {
            Ok(Some(_data)) => {
                latencies.push(start.elapsed());
                count += 1;
                if count >= total {
                    break;
                }
            }
            Ok(None) => break,
            Err(_) => break, // timeout
        }
    }

    latencies
}

fn bench_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("e2e_throughput");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));

    group.bench_function("50msg_batches_x200", |b| {
        b.to_async(&rt).iter(|| async {
            let url = start_blast_server().await;

            let config = Config {
                api_key: "bench".into(),
                feed: Feed::RealTime,
                market: Market::Stocks,
                max_retries: Some(0),
                raw_data: false,
                bypass_raw_data_routing: false,
                reconnect_callback: None,
                url_override: Some(url),
            };

            let (client, output, _errors) = Client::new(config).unwrap();
            client.subscribe(Topic::StocksTrades, &["*"]).await.unwrap();
            client.connect().await.unwrap();

            let start = Instant::now();
            let latencies = drain_output(output, TOTAL_MESSAGES, start).await;

            client.close().await;

            // Report stats (visible in criterion's verbose output).
            if !latencies.is_empty() {
                let total_time = *latencies.last().unwrap();
                let msgs_per_sec = latencies.len() as f64 / total_time.as_secs_f64();

                // p99 latency: time from start until the p99th message arrives.
                let p99_idx = (latencies.len() as f64 * 0.99) as usize;
                let p99 = latencies.get(p99_idx).copied().unwrap_or(total_time);

                eprintln!(
                    "  [{} msgs in {:.2?}] {:.0} msgs/sec | p99={:.2?}",
                    latencies.len(),
                    total_time,
                    msgs_per_sec,
                    p99,
                );
            }
        });
    });

    group.finish();
}

criterion_group!(benches, bench_throughput);
criterion_main!(benches);
