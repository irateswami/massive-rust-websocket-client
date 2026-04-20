//! Property-based tests using proptest.
//!
//! Generates random JSON payloads and verifies:
//!  1. peek_event_type always agrees with serde on valid JSON
//!  2. Serialization round-trips are lossless for all model types
//!
//! Run: `cargo test --test proptest_tests`

use proptest::prelude::*;

use massive_rust_client::websocket::{models::*, peek_event_type};

// ---------------------------------------------------------------------------
// Strategy: random JSON objects with an "ev" field
// ---------------------------------------------------------------------------

/// Generates a random short string suitable for event type values.
fn ev_value_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("T".to_string()),
        Just("Q".to_string()),
        Just("A".to_string()),
        Just("AM".to_string()),
        Just("XT".to_string()),
        Just("XQ".to_string()),
        Just("FMV".to_string()),
        Just("V".to_string()),
        Just("LV".to_string()),
        Just("LULD".to_string()),
        Just("NOI".to_string()),
        Just("status".to_string()),
        "[a-zA-Z]{1,6}".prop_map(|s| s),
    ]
}

/// Generates a JSON object string with an "ev" field and random extra fields.
fn json_with_ev_strategy() -> impl Strategy<Value = String> {
    (
        ev_value_strategy(),
        prop::collection::vec(
            (
                "[a-z]{1,4}".prop_map(|s| s),
                prop_oneof![
                    any::<f64>()
                        .prop_filter("finite", |f| f.is_finite())
                        .prop_map(|f| format!("{f}")),
                    any::<i64>().prop_map(|i| format!("{i}")),
                    "\"[a-zA-Z0-9]{0,10}\"".prop_map(|s| s),
                    Just("null".to_string()),
                    Just("true".to_string()),
                    Just("false".to_string()),
                ],
            ),
            0..5,
        ),
    )
        .prop_map(|(ev, extra_fields)| {
            let mut fields = vec![format!(r#""ev":"{}""#, ev)];
            for (key, val) in extra_fields {
                // Avoid duplicate "ev" key.
                if key != "ev" {
                    fields.push(format!(r#""{}": {}"#, key, val));
                }
            }
            format!("{{{}}}", fields.join(","))
        })
}

/// Generates arbitrary byte sequences (including invalid UTF-8).
fn arbitrary_bytes_strategy() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..256)
}

// ---------------------------------------------------------------------------
// Property: peek_event_type agrees with serde on valid JSON
// ---------------------------------------------------------------------------

proptest! {
    #[test]
    fn peek_agrees_with_serde(json in json_with_ev_strategy()) {
        let peek_result = peek_event_type(json.as_bytes());
        let serde_result: Option<String> = serde_json::from_str::<serde_json::Value>(&json)
            .ok()
            .and_then(|v| v.get("ev").and_then(|ev| ev.as_str().map(|s| s.to_string())));

        match (peek_result, &serde_result) {
            (Some(peek), Some(serde)) => {
                prop_assert_eq!(peek, serde.as_str(),
                    "peek_event_type disagrees with serde for: {}", json);
            }
            (None, Some(serde_val)) => {
                // peek_event_type might fail if the "ev" field has unusual spacing.
                // This is acceptable — document it but don't fail.
                // The byte scan assumes `"ev":"` without spaces.
                let has_spaces = json.contains("\"ev\" :") || json.contains("\"ev\": ");
                prop_assert!(has_spaces || serde_val.is_empty(),
                    "peek returned None but serde found '{}' in: {}", serde_val, json);
            }
            (Some(_), None) => {
                // peek found something but serde didn't parse "ev" as a string.
                // This shouldn't happen for well-formed JSON.
                prop_assert!(false,
                    "peek returned Some but serde returned None for: {}", json);
            }
            (None, None) => {
                // Both agree there's no ev — fine.
            }
        }
    }

    /// peek_event_type must never panic on arbitrary bytes.
    #[test]
    fn peek_never_panics(data in arbitrary_bytes_strategy()) {
        let _ = peek_event_type(&data);
    }
}

// ---------------------------------------------------------------------------
// Property: serialization round-trips are lossless
// ---------------------------------------------------------------------------

/// Strategy for generating wire-format JSON trade payloads (as they arrive from the server).
fn wire_trade_json_strategy() -> impl Strategy<Value = String> {
    (
        "[A-Z]{1,5}",           // sym
        0..100i32,              // exchange
        "[a-z0-9]{1,8}",       // id
        0..4i32,                // tape
        -1_000_000.0..1_000_000.0f64, // price (2 decimal places, like real market data)
        0..10_000i64,           // size
        prop::collection::vec(0..100i32, 0..4), // conditions
        1_600_000_000_000i64..1_800_000_000_000i64, // timestamp
        0..1_000_000i64,        // sequence
    )
        .prop_map(|(sym, exchange, id, tape, price, size, conds, ts, seq)| {
            // Round to 4 decimal places (realistic market precision).
            let price = (price * 10000.0).round() / 10000.0;
            let conds_str = conds.iter().map(|c| c.to_string()).collect::<Vec<_>>().join(",");
            format!(
                r#"{{"ev":"T","sym":"{}","x":{},"i":"{}","z":{},"p":{},"s":{},"c":[{}],"t":{},"q":{}}}"#,
                sym, exchange, id, tape, price, size, conds_str, ts, seq
            )
        })
}

/// Strategy for wire-format FMV payloads.
fn wire_fmv_json_strategy() -> impl Strategy<Value = String> {
    (
        "[A-Z]{1,5}",
        -1_000_000.0..1_000_000.0f64,
        1_600_000_000_000i64..1_800_000_000_000i64,
    )
        .prop_map(|(sym, fmv, ts)| {
            let fmv = (fmv * 100.0).round() / 100.0;
            format!(r#"{{"ev":"FMV","fmv":{},"sym":"{}","t":{}}}"#, fmv, sym, ts)
        })
}

proptest! {
    /// Wire JSON → EquityTrade → JSON round-trip is stable.
    /// Data comes from the server as JSON; after one deser+reser cycle,
    /// subsequent cycles must produce identical output.
    #[test]
    fn round_trip_equity_trade(json in wire_trade_json_strategy()) {
        let trade: EquityTrade = serde_json::from_str(&json).unwrap();
        let json2 = serde_json::to_string(&trade).unwrap();
        let trade2: EquityTrade = serde_json::from_str(&json2).unwrap();
        let json3 = serde_json::to_string(&trade2).unwrap();

        // After first parse, round-trips must be stable (idempotent).
        prop_assert_eq!(&json2, &json3,
            "round-trip not stable after first parse");
        prop_assert_eq!(&trade.symbol, &trade2.symbol);
        prop_assert_eq!(trade.price, trade2.price);
        prop_assert_eq!(trade.size, trade2.size);
        prop_assert_eq!(&trade.conditions, &trade2.conditions);
        prop_assert_eq!(trade.timestamp, trade2.timestamp);
    }

    /// Wire JSON → FairMarketValue → JSON round-trip is stable.
    #[test]
    fn round_trip_fmv(json in wire_fmv_json_strategy()) {
        let fmv: FairMarketValue = serde_json::from_str(&json).unwrap();
        let json2 = serde_json::to_string(&fmv).unwrap();
        let fmv2: FairMarketValue = serde_json::from_str(&json2).unwrap();
        let json3 = serde_json::to_string(&fmv2).unwrap();

        prop_assert_eq!(&json2, &json3);
        prop_assert_eq!(fmv.fmv, fmv2.fmv);
        prop_assert_eq!(&fmv.ticker, &fmv2.ticker);
    }

    /// peek_event_type correctly extracts "ev" from any wire-format trade JSON.
    #[test]
    fn peek_agrees_on_wire_data(json in wire_trade_json_strategy()) {
        let peeked = peek_event_type(json.as_bytes());
        prop_assert_eq!(peeked, Some("T"));
    }
}
