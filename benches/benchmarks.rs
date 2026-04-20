use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use massive_rust_client::websocket::models::*;

// ---------------------------------------------------------------------------
// Fixtures: realistic JSON payloads matching the wire format
// ---------------------------------------------------------------------------

const EQUITY_TRADE_JSON: &str = r#"{"ev":"T","sym":"SPY","x":1,"i":"tr123","z":3,"p":450.25,"s":100,"c":[14,41],"t":1700000000000,"q":42}"#;

const EQUITY_AGG_JSON: &str = r#"{"ev":"AM","sym":"AAPL","v":50000.0,"av":1200000.0,"op":150.0,"vw":151.2,"o":150.5,"c":151.0,"h":152.0,"l":149.0,"a":150.8,"z":120.0,"s":1700000000000,"e":1700000060000}"#;

const EQUITY_QUOTE_JSON: &str = r#"{"ev":"Q","sym":"AAPL","bx":1,"bp":149.95,"bs":5,"ax":2,"ap":150.05,"as":3,"c":0,"t":1700000000000,"z":1,"q":100}"#;

const CRYPTO_TRADE_JSON: &str = r#"{"ev":"XT","pair":"BTC-USD","x":1,"i":"ct456","p":65000.0,"s":0.5,"c":[2],"t":1700000000000,"r":1700000000001}"#;

const FMV_JSON: &str = r#"{"ev":"FMV","fmv":155.42,"sym":"AAPL","t":1700000000000}"#;

const INDEX_VALUE_JSON: &str = r#"{"ev":"V","val":4500.5,"T":"SPX","t":1700000000000}"#;

/// A single-element server array (most common case).
const SERVER_ARRAY_1: &str =
    r#"[{"ev":"T","sym":"SPY","p":450.25,"s":100,"t":1700000000000,"q":1}]"#;

/// A 10-element batch (realistic burst).
fn server_array_10() -> String {
    let elements: Vec<String> = (0..10)
        .map(|i| {
            format!(
                r#"{{"ev":"T","sym":"SPY","p":{},"s":{},"t":1700000000000,"q":{}}}"#,
                450.0 + i as f64 * 0.01,
                100 + i,
                i
            )
        })
        .collect();
    format!("[{}]", elements.join(","))
}

/// A 50-element batch (heavy burst).
fn server_array_50() -> String {
    let elements: Vec<String> = (0..50)
        .map(|i| {
            format!(
                r#"{{"ev":"T","sym":"SPY","p":{},"s":{},"t":1700000000000,"q":{}}}"#,
                450.0 + i as f64 * 0.01,
                100 + i,
                i
            )
        })
        .collect();
    format!("[{}]", elements.join(","))
}

// ---------------------------------------------------------------------------
// Benchmarks: model deserialization (the innermost hot-path operation)
// ---------------------------------------------------------------------------

fn bench_model_deserialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("model_deser");

    group.bench_function("equity_trade", |b| {
        b.iter(|| serde_json::from_str::<EquityTrade>(EQUITY_TRADE_JSON).unwrap());
    });

    group.bench_function("equity_agg", |b| {
        b.iter(|| serde_json::from_str::<EquityAgg>(EQUITY_AGG_JSON).unwrap());
    });

    group.bench_function("equity_quote", |b| {
        b.iter(|| serde_json::from_str::<EquityQuote>(EQUITY_QUOTE_JSON).unwrap());
    });

    group.bench_function("crypto_trade", |b| {
        b.iter(|| serde_json::from_str::<CryptoTrade>(CRYPTO_TRADE_JSON).unwrap());
    });

    group.bench_function("fmv", |b| {
        b.iter(|| serde_json::from_str::<FairMarketValue>(FMV_JSON).unwrap());
    });

    group.bench_function("index_value", |b| {
        b.iter(|| serde_json::from_str::<IndexValue>(INDEX_VALUE_JSON).unwrap());
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmarks: outer array parse (Vec<Box<RawValue>>)
// ---------------------------------------------------------------------------

fn bench_array_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_parse");
    let arr_10 = server_array_10();
    let arr_50 = server_array_50();

    for (name, data) in [
        ("1_element", SERVER_ARRAY_1.to_string()),
        ("10_elements", arr_10),
        ("50_elements", arr_50),
    ] {
        group.throughput(Throughput::Bytes(data.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(name), &data, |b, data| {
            b.iter(|| {
                let _msgs: Vec<Box<serde_json::value::RawValue>> =
                    serde_json::from_str(data).unwrap();
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmarks: event-type peek (current: serde parse vs potential byte scan)
// ---------------------------------------------------------------------------

fn bench_event_type_peek(c: &mut Criterion) {
    let mut group = c.benchmark_group("ev_peek");

    // Current approach: full serde parse into EventType struct.
    group.bench_function("serde_peek", |b| {
        b.iter(|| {
            let ev: serde_json::Value = serde_json::from_str(EQUITY_TRADE_JSON).unwrap();
            let _ = ev.get("ev").and_then(|v| v.as_str());
        });
    });

    // Alternative: byte-level scan for "ev" field.
    group.bench_function("byte_scan", |b| {
        b.iter(|| {
            let _ = peek_ev_byte_scan(EQUITY_TRADE_JSON.as_bytes());
        });
    });

    group.finish();
}

/// Byte-scan approach to extract the "ev" field value.
fn peek_ev_byte_scan(raw: &[u8]) -> Option<&str> {
    let needle = b"\"ev\":\"";
    let pos = raw.windows(needle.len()).position(|w| w == needle)?;
    let start = pos + needle.len();
    let end = raw[start..].iter().position(|&b| b == b'"')? + start;
    std::str::from_utf8(&raw[start..end]).ok()
}

// ---------------------------------------------------------------------------
// Benchmarks: full message pipeline (array parse → ev peek → deser)
// ---------------------------------------------------------------------------

fn bench_full_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("full_pipeline");

    // Single trade through the full pipeline.
    group.bench_function("single_trade", |b| {
        let data = SERVER_ARRAY_1.as_bytes();
        b.iter(|| {
            let msgs: Vec<Box<serde_json::value::RawValue>> = serde_json::from_slice(data).unwrap();
            for raw_msg in &msgs {
                let raw = raw_msg.get();
                let _ = peek_ev_byte_scan(raw.as_bytes());
                let _trade: EquityTrade = serde_json::from_str(raw).unwrap();
            }
        });
    });

    // 10-trade batch.
    let arr_10 = server_array_10();
    group.throughput(Throughput::Elements(10));
    group.bench_function("batch_10_trades", |b| {
        let data = arr_10.as_bytes();
        b.iter(|| {
            let msgs: Vec<Box<serde_json::value::RawValue>> = serde_json::from_slice(data).unwrap();
            for raw_msg in &msgs {
                let raw = raw_msg.get();
                let _ = peek_ev_byte_scan(raw.as_bytes());
                let _trade: EquityTrade = serde_json::from_str(raw).unwrap();
            }
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmarks: subscription operations
// ---------------------------------------------------------------------------

fn bench_subscription(c: &mut Criterion) {
    use massive_rust_client::websocket::config::Topic;

    let mut group = c.benchmark_group("subscription");

    // Build a subscribe message with multiple tickers.
    group.bench_function("build_message_5_tickers", |b| {
        let tickers: Vec<String> = vec![
            "AAPL".into(),
            "TSLA".into(),
            "MSFT".into(),
            "GOOG".into(),
            "AMZN".into(),
        ];
        b.iter(|| {
            let _msg = serde_json::to_string(&serde_json::json!({
                "action": "subscribe",
                "params": tickers.iter()
                    .map(|t| format!("{}.{}", Topic::StocksTrades.prefix(), t))
                    .collect::<Vec<_>>()
                    .join(","),
            }))
            .unwrap();
        });
    });

    // Build a subscribe message for wildcard.
    group.bench_function("build_message_wildcard", |b| {
        b.iter(|| {
            let _msg = serde_json::to_string(&serde_json::json!({
                "action": "subscribe",
                "params": format!("{}.*", Topic::StocksTrades.prefix()),
            }))
            .unwrap();
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_model_deserialization,
    bench_array_parse,
    bench_event_type_peek,
    bench_full_pipeline,
    bench_subscription,
);
criterion_main!(benches);
