#![no_main]

use libfuzzer_sys::fuzz_target;
use massive_rust_client::websocket::models::*;

fuzz_target!(|data: &[u8]| {
    // Feed arbitrary bytes into every model deserializer.
    // Must never panic — only Err is acceptable for invalid input.
    if let Ok(s) = std::str::from_utf8(data) {
        let _ = serde_json::from_str::<EquityTrade>(s);
        let _ = serde_json::from_str::<EquityAgg>(s);
        let _ = serde_json::from_str::<EquityQuote>(s);
        let _ = serde_json::from_str::<CryptoTrade>(s);
        let _ = serde_json::from_str::<CryptoQuote>(s);
        let _ = serde_json::from_str::<CurrencyAgg>(s);
        let _ = serde_json::from_str::<ForexQuote>(s);
        let _ = serde_json::from_str::<Imbalance>(s);
        let _ = serde_json::from_str::<LimitUpLimitDown>(s);
        let _ = serde_json::from_str::<Level2Book>(s);
        let _ = serde_json::from_str::<IndexValue>(s);
        let _ = serde_json::from_str::<LaunchpadValue>(s);
        let _ = serde_json::from_str::<FairMarketValue>(s);
        let _ = serde_json::from_str::<FuturesTrade>(s);
        let _ = serde_json::from_str::<FuturesQuote>(s);
        let _ = serde_json::from_str::<FuturesAggregate>(s);
        let _ = serde_json::from_str::<ControlMessage>(s);

        // Also fuzz the outer array parse (server message envelope).
        let _ = serde_json::from_str::<Vec<Box<serde_json::value::RawValue>>>(s);
    }
});
