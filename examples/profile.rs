//! CPU profiling target for samply.
//!
//! Run with:
//!   cargo build --example profile --release
//!   samply record target/release/examples/profile

use massive_rust_client::websocket::models::*;

const ITERATIONS: usize = 100_000;

fn server_batch() -> String {
    let elements: Vec<String> = (0..10)
        .map(|i| {
            format!(
                r#"{{"ev":"T","sym":"SPY","x":1,"i":"t{i}","z":3,"p":{},"s":{},"c":[14,41],"t":1700000000000,"q":{i}}}"#,
                450.0 + i as f64 * 0.01,
                100 + i,
            )
        })
        .collect();
    format!("[{}]", elements.join(","))
}

#[inline(never)]
fn parse_outer_array(data: &[u8]) -> Vec<Box<serde_json::value::RawValue>> {
    serde_json::from_slice(data).unwrap()
}

#[inline(never)]
fn peek_event_type(raw: &str) -> String {
    let ev: serde_json::Value = serde_json::from_str(raw).unwrap();
    ev.get("ev")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

#[inline(never)]
fn deserialize_trade(raw: &str) -> EquityTrade {
    serde_json::from_str(raw).unwrap()
}

fn main() {
    let batch = server_batch();
    let batch_bytes = batch.as_bytes();

    for _ in 0..ITERATIONS {
        let msgs = parse_outer_array(batch_bytes);
        for raw_msg in &msgs {
            let ev = peek_event_type(raw_msg.get());
            if ev == "T" {
                std::hint::black_box(deserialize_trade(raw_msg.get()));
            }
        }
    }

    eprintln!(
        "Processed {} batches ({} messages)",
        ITERATIONS,
        ITERATIONS * 10
    );
}
