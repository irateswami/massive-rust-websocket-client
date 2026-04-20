//! Heap profiling the hot path with dhat.
//!
//! Run with:
//!   cargo run --example heap_profile --features dhat-heap
//!
//! Then open the generated `dhat-heap.json` in the DHAT viewer:
//!   https://nnethercote.github.io/dh_view/dh_view.html

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use massive_rust_client::websocket::models::*;
use massive_rust_client::websocket::{json_array_elements, peek_event_type};

/// Simulates the process loop hot path: array iterate → ev peek → deser.
fn simulate_hot_path(iterations: usize) {
    // Realistic server message: array of 5 mixed events.
    let server_msg = r#"[
        {"ev":"T","sym":"SPY","x":1,"i":"t1","z":3,"p":450.25,"s":100,"c":[14],"t":1700000000000,"q":1},
        {"ev":"Q","sym":"SPY","bx":1,"bp":449.95,"bs":5,"ax":2,"ap":450.05,"as":3,"c":0,"t":1700000000001,"z":1,"q":2},
        {"ev":"AM","sym":"AAPL","v":50000.0,"av":1200000.0,"op":150.0,"vw":151.2,"o":150.5,"c":151.0,"h":152.0,"l":149.0,"a":150.8,"z":120.0,"s":1700000000000,"e":1700000060000},
        {"ev":"FMV","fmv":155.42,"sym":"AAPL","t":1700000000000},
        {"ev":"T","sym":"TSLA","x":1,"i":"t2","z":3,"p":250.0,"s":50,"c":[14,41],"t":1700000000002,"q":3}
    ]"#;

    for _ in 0..iterations {
        // Step 1: zero-alloc iteration over JSON array elements (matches actual process_loop)
        for raw in json_array_elements(server_msg) {
            // Step 2: peek at "ev" field (zero-alloc byte scan)
            let event_type = peek_event_type(raw.as_bytes()).unwrap_or("");

            // Step 3: full deserialization based on event type
            match event_type {
                "T" => {
                    let _trade: EquityTrade = serde_json::from_str(raw).unwrap();
                }
                "Q" => {
                    let _quote: EquityQuote = serde_json::from_str(raw).unwrap();
                }
                "A" | "AM" => {
                    let _agg: EquityAgg = serde_json::from_str(raw).unwrap();
                }
                "FMV" => {
                    let _fmv: FairMarketValue = serde_json::from_str(raw).unwrap();
                }
                _ => {}
            }
        }
    }
}

fn main() {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    // Simulate 1000 server messages (5000 individual events).
    simulate_hot_path(1000);

    println!("Done. Profile written to dhat-heap.json");
}
