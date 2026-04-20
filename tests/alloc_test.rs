//! Allocation-counting tests for the hot path.
//!
//! Verifies that the critical parsing path (peek_event_type + model deserialization)
//! allocates within expected bounds. Catches regressions that benchmarks might miss
//! as noise.
//!
//! Run: `cargo test --test alloc_test`

use std::alloc::System;

use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

use massive_rust_client::websocket::{models::EquityTrade, peek_event_type};

const TRADE_JSON: &str = r#"{"ev":"T","sym":"SPY","x":1,"i":"tr1","z":3,"p":450.25,"s":100,"c":[14,41],"t":1700000000000,"q":42}"#;

/// peek_event_type should be near-zero-alloc.
/// memchr::memmem::find builds a small internal Finder per call (~23 bytes in debug).
/// In release mode this is typically optimized to zero heap allocs.
/// We assert a ceiling of 64 bytes/call to catch real regressions (accidental
/// String/Vec/Box allocations would be 100+ bytes/call) while allowing memchr internals.
#[test]
fn peek_event_type_near_zero_alloc() {
    // Warm up.
    let _ = peek_event_type(TRADE_JSON.as_bytes());

    let reg = Region::new(GLOBAL);
    let iterations = 1000;
    for _ in 0..iterations {
        let _ = peek_event_type(TRADE_JSON.as_bytes());
    }
    let stats = reg.change();

    let bytes_per_call = stats.bytes_allocated as f64 / iterations as f64;
    assert!(
        bytes_per_call <= 64.0,
        "peek_event_type allocated {:.1} bytes/call (max: 64). \
         Total: {} bytes over {} calls. \
         Likely a regression — new heap allocations in the hot path.",
        bytes_per_call,
        stats.bytes_allocated,
        iterations,
    );

    // Report actual cost for visibility.
    eprintln!(
        "  [alloc info] peek_event_type: {:.1} bytes/call ({} total over {} calls)",
        bytes_per_call, stats.bytes_allocated, iterations
    );
}

/// Model deserialization: assert a bounded number of allocations.
/// serde_json + CompactString + SmallVec should be near-zero alloc for typical payloads.
/// SmallVec<[i32; 4]> inlines conditions, CompactString inlines short symbols.
#[test]
fn trade_deser_bounded_allocs() {
    // Warm up.
    let _ = serde_json::from_str::<EquityTrade>(TRADE_JSON).unwrap();

    let reg = Region::new(GLOBAL);
    let iterations = 1000;
    for _ in 0..iterations {
        let trade: EquityTrade = serde_json::from_str(TRADE_JSON).unwrap();
        // Prevent the compiler from optimizing away the deserialization.
        std::hint::black_box(&trade);
    }
    let stats = reg.change();

    // Each trade deser should allocate at most:
    //  - SmallVec<[i32; 4]> for conditions → 0 allocs for ≤4 conditions (inline)
    //  - Strings within CompactString inline capacity (24 bytes) → 0 allocs
    // With SmallVec, typical trades should be zero-alloc. Allow headroom for platform variance.
    let allocs_per_iter = stats.allocations as f64 / iterations as f64;
    assert!(
        allocs_per_iter <= 3.0,
        "trade deserialization averages {:.2} allocs/iter (max allowed: 3.0). \
         Total: {} allocs over {} iterations",
        allocs_per_iter,
        stats.allocations,
        iterations,
    );
}

/// Full pipeline: array parse → peek → deser. Ensure no surprise quadratic allocations.
#[test]
fn full_pipeline_bounded_allocs() {
    let batch_10: String = {
        let elements: Vec<String> = (0..10)
            .map(|i| {
                format!(
                    r#"{{"ev":"T","sym":"SPY","p":{},"s":{},"t":1700000000000,"q":{}}}"#,
                    450.0 + i as f64 * 0.01,
                    100 + i,
                    i,
                )
            })
            .collect();
        format!("[{}]", elements.join(","))
    };

    // Warm up.
    let msgs: Vec<&serde_json::value::RawValue> = serde_json::from_str(&batch_10).unwrap();
    for raw in &msgs {
        let _ = peek_event_type(raw.get().as_bytes());
        let _: EquityTrade = serde_json::from_str(raw.get()).unwrap();
    }

    let reg = Region::new(GLOBAL);
    let iterations = 100;
    for _ in 0..iterations {
        let msgs: Vec<&serde_json::value::RawValue> = serde_json::from_str(&batch_10).unwrap();
        for raw in &msgs {
            let _ = peek_event_type(raw.get().as_bytes());
            let trade: EquityTrade = serde_json::from_str(raw.get()).unwrap();
            std::hint::black_box(&trade);
        }
    }
    let stats = reg.change();

    // 10 messages per iteration using &RawValue (borrows from source, no per-element Box).
    // Allocations: 1 Vec pointer array + SmallVec inlines conditions → near-zero per-element.
    // Budget: 20 allocs per iteration of 10 messages.
    let allocs_per_iter = stats.allocations as f64 / iterations as f64;
    assert!(
        allocs_per_iter <= 20.0,
        "full pipeline averages {:.1} allocs/iter for 10-msg batch (max: 20). \
         Total: {} allocs over {} iterations",
        allocs_per_iter,
        stats.allocations,
        iterations,
    );
}
