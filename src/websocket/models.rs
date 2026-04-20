use compact_str::CompactString;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

/// Control message for status and control events from the server.
#[derive(Debug, Clone, Deserialize)]
pub struct ControlMessage {
    #[serde(rename = "ev", default)]
    pub event_type: CompactString,
    #[serde(default)]
    pub status: CompactString,
    #[serde(default)]
    pub message: CompactString,
}

/// Aggregate for stock tickers or option contracts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EquityAgg {
    #[serde(rename = "ev", default)]
    pub event_type: CompactString,
    #[serde(rename = "sym", default)]
    pub symbol: CompactString,
    #[serde(rename = "v", default)]
    pub volume: f64,
    #[serde(rename = "av", default)]
    pub accumulated_volume: f64,
    #[serde(rename = "op", default)]
    pub official_open_price: f64,
    #[serde(rename = "vw", default)]
    pub vwap: f64,
    #[serde(rename = "o", default)]
    pub open: f64,
    #[serde(rename = "c", default)]
    pub close: f64,
    #[serde(rename = "h", default)]
    pub high: f64,
    #[serde(rename = "l", default)]
    pub low: f64,
    #[serde(rename = "a", default)]
    pub aggregate_vwap: f64,
    #[serde(rename = "z", default)]
    pub average_size: f64,
    #[serde(rename = "s", default)]
    pub start_timestamp: i64,
    #[serde(rename = "e", default)]
    pub end_timestamp: i64,
    #[serde(rename = "otc", default)]
    pub otc: bool,
}

/// Aggregate for forex currency pairs or crypto pairs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurrencyAgg {
    #[serde(rename = "ev", default)]
    pub event_type: CompactString,
    #[serde(rename = "pair", default)]
    pub pair: CompactString,
    #[serde(rename = "o", default)]
    pub open: f64,
    #[serde(rename = "c", default)]
    pub close: f64,
    #[serde(rename = "h", default)]
    pub high: f64,
    #[serde(rename = "l", default)]
    pub low: f64,
    #[serde(rename = "v", default)]
    pub volume: f64,
    #[serde(rename = "vw", default)]
    pub vwap: f64,
    #[serde(rename = "s", default)]
    pub start_timestamp: i64,
    #[serde(rename = "e", default)]
    pub end_timestamp: i64,
    #[serde(rename = "z", default)]
    pub avg_trade_size: i32,
}

/// Trade data for stock tickers or option contracts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EquityTrade {
    #[serde(rename = "ev", default)]
    pub event_type: CompactString,
    #[serde(rename = "sym", default)]
    pub symbol: CompactString,
    #[serde(rename = "x", default)]
    pub exchange: i32,
    #[serde(rename = "i", default)]
    pub id: CompactString,
    #[serde(rename = "z", default)]
    pub tape: i32,
    #[serde(rename = "p", default)]
    pub price: f64,
    #[serde(rename = "s", default)]
    pub size: i64,
    #[serde(rename = "c", default)]
    pub conditions: SmallVec<[i32; 4]>,
    #[serde(rename = "t", default)]
    pub timestamp: i64,
    #[serde(rename = "q", default)]
    pub sequence_number: i64,
    #[serde(rename = "trfi", default)]
    pub trade_reporting_facility_id: i64,
    #[serde(rename = "trft", default)]
    pub trade_reporting_facility_timestamp: i64,
}

/// Trade for a crypto pair.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CryptoTrade {
    #[serde(rename = "ev", default)]
    pub event_type: CompactString,
    #[serde(rename = "pair", default)]
    pub pair: CompactString,
    #[serde(rename = "x", default)]
    pub exchange: i32,
    #[serde(rename = "i", default)]
    pub id: CompactString,
    #[serde(rename = "p", default)]
    pub price: f64,
    #[serde(rename = "s", default)]
    pub size: f64,
    #[serde(rename = "c", default)]
    pub conditions: SmallVec<[i32; 4]>,
    #[serde(rename = "t", default)]
    pub timestamp: i64,
    #[serde(rename = "r", default)]
    pub received_timestamp: i64,
}

/// Quote for stock tickers or option contracts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EquityQuote {
    #[serde(rename = "ev", default)]
    pub event_type: CompactString,
    #[serde(rename = "sym", default)]
    pub symbol: CompactString,
    #[serde(rename = "bx", default)]
    pub bid_exchange_id: i32,
    #[serde(rename = "bp", default)]
    pub bid_price: f64,
    #[serde(rename = "bs", default)]
    pub bid_size: i32,
    #[serde(rename = "ax", default)]
    pub ask_exchange_id: i32,
    #[serde(rename = "ap", default)]
    pub ask_price: f64,
    #[serde(rename = "as", default)]
    pub ask_size: i32,
    #[serde(rename = "c", default)]
    pub condition: i32,
    #[serde(rename = "i", default)]
    pub indicators: SmallVec<[i32; 4]>,
    #[serde(rename = "t", default)]
    pub timestamp: i64,
    #[serde(rename = "z", default)]
    pub tape: i32,
    #[serde(rename = "q", default)]
    pub sequence_number: i64,
}

/// Quote for a forex currency pair.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForexQuote {
    #[serde(rename = "ev", default)]
    pub event_type: CompactString,
    #[serde(rename = "p", default)]
    pub pair: CompactString,
    #[serde(rename = "x", default)]
    pub exchange_id: i32,
    #[serde(rename = "a", default)]
    pub ask_price: f64,
    #[serde(rename = "b", default)]
    pub bid_price: f64,
    #[serde(rename = "t", default)]
    pub timestamp: i64,
}

/// Quote for a crypto pair.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CryptoQuote {
    #[serde(rename = "ev", default)]
    pub event_type: CompactString,
    #[serde(rename = "pair", default)]
    pub pair: CompactString,
    #[serde(rename = "bp", default)]
    pub bid_price: f64,
    #[serde(rename = "bs", default)]
    pub bid_size: f64,
    #[serde(rename = "ap", default)]
    pub ask_price: f64,
    #[serde(rename = "as", default)]
    pub ask_size: f64,
    #[serde(rename = "t", default)]
    pub timestamp: i64,
    #[serde(rename = "x", default)]
    pub exchange_id: i32,
    #[serde(rename = "r", default)]
    pub received_timestamp: i64,
}

/// Imbalance event for a stock ticker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Imbalance {
    #[serde(rename = "ev", default)]
    pub event_type: CompactString,
    #[serde(rename = "T", default)]
    pub symbol: CompactString,
    #[serde(rename = "t", default)]
    pub timestamp: i64,
    #[serde(rename = "at", default)]
    pub auction_time: i32,
    #[serde(rename = "a", default)]
    pub auction_type: CompactString,
    #[serde(rename = "i", default)]
    pub symbol_sequence: i32,
    #[serde(rename = "x", default)]
    pub exchange_id: i32,
    #[serde(rename = "o", default)]
    pub imbalance_quantity: i32,
    #[serde(rename = "p", default)]
    pub paired_quantity: i32,
    #[serde(rename = "b", default)]
    pub book_clearing_price: f64,
}

/// LULD (Limit Up Limit Down) event for a stock ticker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LimitUpLimitDown {
    #[serde(rename = "ev", default)]
    pub event_type: CompactString,
    #[serde(rename = "T", default)]
    pub symbol: CompactString,
    #[serde(rename = "h", default)]
    pub high_price: f64,
    #[serde(rename = "l", default)]
    pub low_price: f64,
    #[serde(rename = "i", default)]
    pub indicators: SmallVec<[i32; 4]>,
    #[serde(rename = "z", default)]
    pub tape: i32,
    #[serde(rename = "t", default)]
    pub timestamp: i64,
    #[serde(rename = "q", default)]
    pub sequence_number: i64,
}

/// Level 2 book data for a crypto pair.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Level2Book {
    #[serde(rename = "ev", default)]
    pub event_type: CompactString,
    #[serde(rename = "pair", default)]
    pub pair: CompactString,
    #[serde(rename = "b", default)]
    pub bid_prices: Vec<Vec<f64>>,
    #[serde(rename = "a", default)]
    pub ask_prices: Vec<Vec<f64>>,
    #[serde(rename = "t", default)]
    pub timestamp: i64,
    #[serde(rename = "x", default)]
    pub exchange_id: i32,
    #[serde(rename = "r", default)]
    pub received_timestamp: i64,
}

/// Value data for indices.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexValue {
    #[serde(rename = "ev", default)]
    pub event_type: CompactString,
    #[serde(rename = "val", default)]
    pub value: f64,
    #[serde(rename = "T", default)]
    pub ticker: CompactString,
    #[serde(rename = "t", default)]
    pub timestamp: i64,
}

/// Launchpad value data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LaunchpadValue {
    #[serde(rename = "ev", default)]
    pub event_type: CompactString,
    #[serde(rename = "val", default)]
    pub value: f64,
    #[serde(rename = "sym", default)]
    pub ticker: CompactString,
    #[serde(rename = "t", default)]
    pub timestamp: i64,
}

/// Fair market value data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FairMarketValue {
    #[serde(rename = "ev", default)]
    pub event_type: CompactString,
    #[serde(rename = "fmv", default)]
    pub fmv: f64,
    #[serde(rename = "sym", default)]
    pub ticker: CompactString,
    #[serde(rename = "t", default)]
    pub timestamp: i64,
}

/// Futures trade event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FuturesTrade {
    #[serde(rename = "ev", default)]
    pub event_type: CompactString,
    #[serde(rename = "sym", default)]
    pub symbol: CompactString,
    #[serde(rename = "p", default)]
    pub price: f64,
    #[serde(rename = "s", default)]
    pub size: i64,
    #[serde(rename = "t", default)]
    pub timestamp: i64,
    #[serde(rename = "q", default)]
    pub sequence_number: i64,
}

/// Futures quote event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FuturesQuote {
    #[serde(rename = "ev", default)]
    pub event_type: CompactString,
    #[serde(rename = "sym", default)]
    pub symbol: CompactString,
    #[serde(rename = "bp", default)]
    pub bid_price: f64,
    #[serde(rename = "bs", default)]
    pub bid_size: i64,
    #[serde(rename = "bt", default)]
    pub bid_timestamp: i64,
    #[serde(rename = "ap", default)]
    pub ask_price: f64,
    #[serde(rename = "as", default)]
    pub ask_size: i64,
    #[serde(rename = "at", default)]
    pub ask_timestamp: i64,
    #[serde(rename = "t", default)]
    pub timestamp: i64,
}

/// Futures aggregate event (second or minute bars).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FuturesAggregate {
    #[serde(rename = "ev", default)]
    pub event_type: CompactString,
    #[serde(rename = "sym", default)]
    pub symbol: CompactString,
    #[serde(rename = "v", default)]
    pub volume: f64,
    #[serde(rename = "dv", default)]
    pub total_value: f64,
    #[serde(rename = "o", default)]
    pub open: f64,
    #[serde(rename = "c", default)]
    pub close: f64,
    #[serde(rename = "h", default)]
    pub high: f64,
    #[serde(rename = "l", default)]
    pub low: f64,
    #[serde(rename = "n", default)]
    pub transactions: i64,
    #[serde(rename = "s", default)]
    pub start_timestamp: i64,
    #[serde(rename = "e", default)]
    pub end_timestamp: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_equity_agg_deser() {
        let json = r#"{"ev":"A","sym":"AAPL","v":100.0,"av":5000.0,"op":150.0,"vw":150.5,"o":150.5,"c":151.0,"h":152.0,"l":149.0,"a":150.3,"z":50.0,"s":1234567890,"e":1234567891,"otc":false}"#;
        let agg: EquityAgg = serde_json::from_str(json).unwrap();
        assert_eq!(agg.event_type, "A");
        assert_eq!(agg.symbol, "AAPL");
        assert_eq!(agg.volume, 100.0);
        assert_eq!(agg.open, 150.5);
        assert_eq!(agg.close, 151.0);
        assert_eq!(agg.high, 152.0);
        assert_eq!(agg.low, 149.0);
        assert!(!agg.otc);
    }

    #[test]
    fn test_equity_agg_deser_missing_fields() {
        // serde(default) should fill zero values for missing fields.
        let json = r#"{"ev":"AM","sym":"TSLA"}"#;
        let agg: EquityAgg = serde_json::from_str(json).unwrap();
        assert_eq!(agg.event_type, "AM");
        assert_eq!(agg.symbol, "TSLA");
        assert_eq!(agg.volume, 0.0);
        assert_eq!(agg.start_timestamp, 0);
    }

    #[test]
    fn test_equity_trade_deser() {
        let json = r#"{"ev":"T","sym":"SPY","x":1,"i":"trade123","z":3,"p":450.25,"s":100,"c":[14,41],"t":1700000000000,"q":42}"#;
        let trade: EquityTrade = serde_json::from_str(json).unwrap();
        assert_eq!(trade.event_type, "T");
        assert_eq!(trade.symbol, "SPY");
        assert_eq!(trade.exchange, 1);
        assert_eq!(trade.id, "trade123");
        assert_eq!(trade.price, 450.25);
        assert_eq!(trade.size, 100);
        assert_eq!(trade.conditions.as_slice(), &[14, 41]);
        assert_eq!(trade.sequence_number, 42);
    }

    #[test]
    fn test_equity_quote_deser() {
        let json = r#"{"ev":"Q","sym":"AAPL","bx":1,"bp":149.0,"bs":5,"ax":2,"ap":149.5,"as":3,"c":0,"t":1700000000000,"z":1,"q":100}"#;
        let quote: EquityQuote = serde_json::from_str(json).unwrap();
        assert_eq!(quote.event_type, "Q");
        assert_eq!(quote.symbol, "AAPL");
        assert_eq!(quote.bid_price, 149.0);
        assert_eq!(quote.ask_price, 149.5);
        assert_eq!(quote.bid_size, 5);
        assert_eq!(quote.ask_size, 3);
    }

    #[test]
    fn test_currency_agg_deser() {
        let json = r#"{"ev":"CA","pair":"EUR/USD","o":1.085,"c":1.086,"h":1.087,"l":1.084,"v":50000.0,"vw":1.0855,"s":1700000000000,"e":1700000060000,"z":25}"#;
        let agg: CurrencyAgg = serde_json::from_str(json).unwrap();
        assert_eq!(agg.event_type, "CA");
        assert_eq!(agg.pair, "EUR/USD");
        assert_eq!(agg.open, 1.085);
        assert_eq!(agg.avg_trade_size, 25);
    }

    #[test]
    fn test_crypto_trade_deser() {
        let json = r#"{"ev":"XT","pair":"BTC-USD","x":1,"p":65000.0,"s":0.5,"c":[2],"t":1700000000000,"r":1700000000001}"#;
        let trade: CryptoTrade = serde_json::from_str(json).unwrap();
        assert_eq!(trade.event_type, "XT");
        assert_eq!(trade.pair, "BTC-USD");
        assert_eq!(trade.price, 65000.0);
        assert_eq!(trade.size, 0.5);
        assert_eq!(trade.received_timestamp, 1700000000001);
    }

    #[test]
    fn test_forex_quote_deser() {
        let json = r#"{"ev":"C","p":"GBP/USD","x":1,"a":1.27,"b":1.269,"t":1700000000000}"#;
        let quote: ForexQuote = serde_json::from_str(json).unwrap();
        assert_eq!(quote.event_type, "C");
        assert_eq!(quote.pair, "GBP/USD");
        assert_eq!(quote.ask_price, 1.27);
        assert_eq!(quote.bid_price, 1.269);
    }

    #[test]
    fn test_imbalance_deser() {
        let json = r#"{"ev":"NOI","T":"AAPL","t":1700000000000,"at":930,"a":"O","i":1,"x":1,"o":500,"p":1000,"b":150.0}"#;
        let imb: Imbalance = serde_json::from_str(json).unwrap();
        assert_eq!(imb.event_type, "NOI");
        assert_eq!(imb.symbol, "AAPL");
        assert_eq!(imb.auction_time, 930);
        assert_eq!(imb.auction_type, "O");
        assert_eq!(imb.imbalance_quantity, 500);
    }

    #[test]
    fn test_luld_deser() {
        let json = r#"{"ev":"LULD","T":"TSLA","h":300.0,"l":250.0,"i":[1],"z":3,"t":1700000000000,"q":99}"#;
        let luld: LimitUpLimitDown = serde_json::from_str(json).unwrap();
        assert_eq!(luld.event_type, "LULD");
        assert_eq!(luld.symbol, "TSLA");
        assert_eq!(luld.high_price, 300.0);
        assert_eq!(luld.low_price, 250.0);
    }

    #[test]
    fn test_level2_book_deser() {
        let json = r#"{"ev":"XL2","pair":"BTC-USD","b":[[65000.0,1.5],[64999.0,2.0]],"a":[[65001.0,0.5]],"t":1700000000000,"x":1,"r":1700000000001}"#;
        let book: Level2Book = serde_json::from_str(json).unwrap();
        assert_eq!(book.event_type, "XL2");
        assert_eq!(book.pair, "BTC-USD");
        assert_eq!(book.bid_prices.len(), 2);
        assert_eq!(book.ask_prices.len(), 1);
        assert_eq!(book.bid_prices[0], vec![65000.0, 1.5]);
    }

    #[test]
    fn test_index_value_deser() {
        let json = r#"{"ev":"V","val":4500.5,"T":"SPX","t":1700000000000}"#;
        let idx: IndexValue = serde_json::from_str(json).unwrap();
        assert_eq!(idx.event_type, "V");
        assert_eq!(idx.value, 4500.5);
        assert_eq!(idx.ticker, "SPX");
    }

    #[test]
    fn test_launchpad_value_deser() {
        let json = r#"{"ev":"LV","val":155.3,"sym":"AAPL","t":1700000000000}"#;
        let lv: LaunchpadValue = serde_json::from_str(json).unwrap();
        assert_eq!(lv.event_type, "LV");
        assert_eq!(lv.value, 155.3);
        assert_eq!(lv.ticker, "AAPL");
    }

    #[test]
    fn test_fair_market_value_deser() {
        let json = r#"{"ev":"FMV","fmv":155.42,"sym":"AAPL","t":1700000000000}"#;
        let fmv: FairMarketValue = serde_json::from_str(json).unwrap();
        assert_eq!(fmv.event_type, "FMV");
        assert_eq!(fmv.fmv, 155.42);
        assert_eq!(fmv.ticker, "AAPL");
    }

    #[test]
    fn test_futures_trade_deser() {
        let json = r#"{"ev":"T","sym":"ESZ4","p":5500.0,"s":10,"t":1700000000000,"q":1}"#;
        let ft: FuturesTrade = serde_json::from_str(json).unwrap();
        assert_eq!(ft.event_type, "T");
        assert_eq!(ft.symbol, "ESZ4");
        assert_eq!(ft.price, 5500.0);
        assert_eq!(ft.size, 10);
    }

    #[test]
    fn test_futures_quote_deser() {
        let json = r#"{"ev":"Q","sym":"ESZ4","bp":5499.0,"bs":50,"bt":1700000000000,"ap":5500.0,"as":30,"at":1700000000000,"t":1700000000000}"#;
        let fq: FuturesQuote = serde_json::from_str(json).unwrap();
        assert_eq!(fq.event_type, "Q");
        assert_eq!(fq.symbol, "ESZ4");
        assert_eq!(fq.bid_price, 5499.0);
        assert_eq!(fq.ask_price, 5500.0);
    }

    #[test]
    fn test_futures_aggregate_deser() {
        let json = r#"{"ev":"A","sym":"ESZ4","v":1000.0,"dv":5500000.0,"o":5490.0,"c":5500.0,"h":5510.0,"l":5480.0,"n":200,"s":1700000000000,"e":1700000001000}"#;
        let fa: FuturesAggregate = serde_json::from_str(json).unwrap();
        assert_eq!(fa.event_type, "A");
        assert_eq!(fa.symbol, "ESZ4");
        assert_eq!(fa.volume, 1000.0);
        assert_eq!(fa.total_value, 5500000.0);
        assert_eq!(fa.transactions, 200);
    }

    #[test]
    fn test_control_message_deser() {
        let json = r#"{"ev":"status","status":"auth_success","message":"authenticated"}"#;
        let cm: ControlMessage = serde_json::from_str(json).unwrap();
        assert_eq!(cm.event_type, "status");
        assert_eq!(cm.status, "auth_success");
        assert_eq!(cm.message, "authenticated");
    }

    #[test]
    fn test_server_array_deser() {
        // Server sends messages as JSON arrays.
        let json = r#"[{"ev":"status","status":"connected","message":"Connected Successfully"},{"ev":"status","status":"auth_success","message":"authenticated"}]"#;
        let msgs: Vec<Box<serde_json::value::RawValue>> = serde_json::from_str(json).unwrap();
        assert_eq!(msgs.len(), 2);

        let cm: ControlMessage = serde_json::from_str(msgs[0].get()).unwrap();
        assert_eq!(cm.status, "connected");
    }
}
