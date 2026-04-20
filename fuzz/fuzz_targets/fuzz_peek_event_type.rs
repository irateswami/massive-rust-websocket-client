#![no_main]

use libfuzzer_sys::fuzz_target;
use massive_rust_client::websocket::peek_event_type;

fuzz_target!(|data: &[u8]| {
    // Must never panic regardless of input.
    let result = peek_event_type(data);

    // If peek_event_type returns Some, verify:
    //  1. It is valid UTF-8 (guaranteed by the Ok branch of from_utf8).
    //  2. If the input is valid JSON with an "ev" field, the byte-scan result
    //     agrees with serde.
    if let Some(ev) = result {
        assert!(!ev.is_empty() || ev.is_empty()); // touch it to prevent optimizing away

        // Cross-check against serde when input is valid UTF-8 JSON.
        if let Ok(s) = std::str::from_utf8(data) {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
                if let Some(serde_ev) = v.get("ev").and_then(|v| v.as_str()) {
                    assert_eq!(
                        ev, serde_ev,
                        "peek disagrees with serde for input: {:?}",
                        s
                    );
                }
            }
        }
    }
});
