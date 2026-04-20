use std::fmt;

use crate::websocket::error::ClientError;

/// Data feed representing the server host.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Feed {
    Delayed,
    RealTime,
    Nasdaq,
    PolyFeed,
    PolyFeedPlus,
    StarterFeed,
    LaunchpadFeed,
    BusinessFeed,
    EdgxBusinessFeed,
    IexBusiness,
    DelayedBusinessFeed,
    DelayedEdgxBusinessFeed,
    DelayedNasdaqLastSaleBusinessFeed,
    DelayedNasdaqBasicFeed,
    DelayedFullMarketBusinessFeed,
    FullMarketBusinessFeed,
    NasdaqLastSaleBusinessFeed,
    NasdaqBasicBusinessFeed,
}

impl Feed {
    /// Returns the WebSocket URL for this feed.
    pub fn url(&self) -> &'static str {
        match self {
            Feed::Delayed => "wss://delayed.massive.com",
            Feed::RealTime => "wss://socket.massive.com",
            Feed::Nasdaq => "wss://nasdaqfeed.massive.com",
            Feed::PolyFeed => "wss://polyfeed.massive.com",
            Feed::PolyFeedPlus => "wss://polyfeedplus.massive.com",
            Feed::StarterFeed => "wss://starterfeed.massive.com",
            Feed::LaunchpadFeed => "wss://launchpad.massive.com",
            Feed::BusinessFeed => "wss://business.massive.com",
            Feed::EdgxBusinessFeed => "wss://edgx-business.massive.com",
            Feed::IexBusiness => "wss://iex-business.massive.com",
            Feed::DelayedBusinessFeed => "wss://delayed-business.massive.com",
            Feed::DelayedEdgxBusinessFeed => "wss://delayed-edgx-business.massive.com",
            Feed::DelayedNasdaqLastSaleBusinessFeed => {
                "wss://delayed-nasdaq-last-sale-business.massive.com"
            }
            Feed::DelayedNasdaqBasicFeed => "wss://delayed-nasdaq-basic-business.massive.com",
            Feed::DelayedFullMarketBusinessFeed => "wss://delayed-fullmarket-business.massive.com",
            Feed::FullMarketBusinessFeed => "wss://fullmarket-business.massive.com",
            Feed::NasdaqLastSaleBusinessFeed => "wss://nasdaq-last-sale-business.massive.com",
            Feed::NasdaqBasicBusinessFeed => "wss://nasdaq-basic-business.massive.com",
        }
    }
}

impl fmt::Display for Feed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.url())
    }
}

/// Market type used to connect to the server.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Market {
    Stocks,
    Options,
    Forex,
    Crypto,
    Indices,
    Futures,
    FuturesCme,
    FuturesCbot,
    FuturesNymex,
    FuturesComex,
}

impl Market {
    /// Returns the URL path segment for this market.
    pub fn url_path(&self) -> &'static str {
        match self {
            Market::Stocks => "stocks",
            Market::Options => "options",
            Market::Forex => "forex",
            Market::Crypto => "crypto",
            Market::Indices => "indices",
            Market::Futures => "futures",
            Market::FuturesCme => "futures/cme",
            Market::FuturesCbot => "futures/cbot",
            Market::FuturesNymex => "futures/nymex",
            Market::FuturesComex => "futures/comex",
        }
    }

    /// Returns whether the given topic is supported for this market.
    pub fn supports(&self, topic: &Topic) -> bool {
        let is_fmv = matches!(topic, Topic::BusinessFairMarketValue);
        match self {
            Market::Stocks => {
                is_fmv
                    || matches!(
                        topic,
                        Topic::StocksSecAggs
                            | Topic::StocksMinAggs
                            | Topic::StocksTrades
                            | Topic::StocksQuotes
                            | Topic::StocksImbalances
                            | Topic::StocksLULD
                            | Topic::StocksLaunchpadMinAggs
                            | Topic::StocksLaunchpadValue
                    )
            }
            Market::Options => {
                is_fmv
                    || matches!(
                        topic,
                        Topic::OptionsSecAggs
                            | Topic::OptionsMinAggs
                            | Topic::OptionsTrades
                            | Topic::OptionsQuotes
                            | Topic::OptionsLaunchpadMinAggs
                            | Topic::OptionsLaunchpadValue
                    )
            }
            Market::Forex => {
                is_fmv
                    || matches!(
                        topic,
                        Topic::ForexSecAggs
                            | Topic::ForexMinAggs
                            | Topic::ForexQuotes
                            | Topic::ForexLaunchpadMinAggs
                            | Topic::ForexLaunchpadValue
                    )
            }
            Market::Crypto => {
                is_fmv
                    || matches!(
                        topic,
                        Topic::CryptoSecAggs
                            | Topic::CryptoMinAggs
                            | Topic::CryptoTrades
                            | Topic::CryptoQuotes
                            | Topic::CryptoL2Book
                            | Topic::CryptoLaunchpadMinAggs
                            | Topic::CryptoLaunchpadValue
                    )
            }
            Market::Indices => matches!(
                topic,
                Topic::IndexSecAggs | Topic::IndexMinAggs | Topic::IndexValue
            ),
            Market::Futures
            | Market::FuturesCme
            | Market::FuturesCbot
            | Market::FuturesNymex
            | Market::FuturesComex => matches!(
                topic,
                Topic::FutureSecAggs
                    | Topic::FutureMinAggs
                    | Topic::FutureTrades
                    | Topic::FutureQuotes
            ),
        }
    }
}

impl fmt::Display for Market {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.url_path())
    }
}

/// Data type used to subscribe and retrieve data from the server.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Topic {
    // Stocks (use with Stocks market or LaunchpadFeed)
    StocksSecAggs,
    StocksMinAggs,
    StocksTrades,
    StocksQuotes,
    StocksImbalances,
    StocksLULD,
    StocksLaunchpadMinAggs,
    StocksLaunchpadValue,

    // Options
    OptionsSecAggs,
    OptionsMinAggs,
    OptionsTrades,
    OptionsQuotes,
    OptionsLaunchpadMinAggs,
    OptionsLaunchpadValue,

    // Forex
    ForexSecAggs,
    ForexMinAggs,
    ForexQuotes,
    ForexLaunchpadMinAggs,
    ForexLaunchpadValue,

    // Crypto
    CryptoSecAggs,
    CryptoMinAggs,
    CryptoTrades,
    CryptoQuotes,
    CryptoL2Book,
    CryptoLaunchpadMinAggs,
    CryptoLaunchpadValue,

    // Indices
    IndexSecAggs,
    IndexMinAggs,
    IndexValue,

    // Business
    BusinessFairMarketValue,

    // Futures
    FutureSecAggs,
    FutureMinAggs,
    FutureTrades,
    FutureQuotes,
}

impl Topic {
    /// Returns the wire-format prefix for this topic.
    pub fn prefix(&self) -> &'static str {
        match self {
            Topic::StocksSecAggs => "A",
            Topic::StocksMinAggs => "AM",
            Topic::StocksTrades => "T",
            Topic::StocksQuotes => "Q",
            Topic::StocksImbalances => "NOI",
            Topic::StocksLULD => "LULD",
            Topic::StocksLaunchpadMinAggs => "AM",
            Topic::StocksLaunchpadValue => "LV",

            Topic::OptionsSecAggs => "A",
            Topic::OptionsMinAggs => "AM",
            Topic::OptionsTrades => "T",
            Topic::OptionsQuotes => "Q",
            Topic::OptionsLaunchpadMinAggs => "AM",
            Topic::OptionsLaunchpadValue => "LV",

            Topic::ForexSecAggs => "CAS",
            Topic::ForexMinAggs => "CA",
            Topic::ForexQuotes => "C",
            Topic::ForexLaunchpadMinAggs => "AM",
            Topic::ForexLaunchpadValue => "LV",

            Topic::CryptoSecAggs => "XAS",
            Topic::CryptoMinAggs => "XA",
            Topic::CryptoTrades => "XT",
            Topic::CryptoQuotes => "XQ",
            Topic::CryptoL2Book => "XL2",
            Topic::CryptoLaunchpadMinAggs => "AM",
            Topic::CryptoLaunchpadValue => "LV",

            Topic::IndexSecAggs => "A",
            Topic::IndexMinAggs => "AM",
            Topic::IndexValue => "V",

            Topic::BusinessFairMarketValue => "FMV",

            Topic::FutureSecAggs => "A",
            Topic::FutureMinAggs => "AM",
            Topic::FutureTrades => "T",
            Topic::FutureQuotes => "Q",
        }
    }
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.prefix())
    }
}

/// Configuration for the WebSocket client.
pub struct Config {
    /// API key used to authenticate against the server.
    pub api_key: String,

    /// Data feed (e.g. Delayed, RealTime) representing the server host.
    pub feed: Feed,

    /// Market type (e.g. Stocks, Crypto) used to connect to the server.
    pub market: Market,

    /// Maximum number of retry attempts. If reached, the client closes.
    /// `None` means reconnect indefinitely.
    pub max_retries: Option<u64>,

    /// Whether data should be returned as raw bytes instead of typed structs.
    pub raw_data: bool,

    /// If `raw_data` is true and this is true, raw bytes are returned directly
    /// without any internal routing (caller handles all message types including auth).
    pub bypass_raw_data_routing: bool,

    /// Callback triggered on automatic reconnects. `None` error means success,
    /// `Some(err)` means a failed attempt being retried.
    pub reconnect_callback: Option<Box<dyn Fn(Option<&ClientError>) + Send + Sync>>,

    /// Optional URL override (for testing). If set, Feed and Market are ignored
    /// for URL construction.
    pub url_override: Option<String>,
}

impl Config {
    pub(crate) fn validate(&self) -> Result<(), ClientError> {
        if self.api_key.is_empty() {
            return Err(ClientError::InvalidConfig("API key is required".into()));
        }
        Ok(())
    }

    pub(crate) fn build_url(&self) -> String {
        if let Some(ref url) = self.url_override {
            return url.clone();
        }
        format!("{}/{}", self.feed.url(), self.market.url_path())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Feed ---

    #[test]
    fn test_feed_urls() {
        assert_eq!(Feed::RealTime.url(), "wss://socket.massive.com");
        assert_eq!(Feed::Delayed.url(), "wss://delayed.massive.com");
        assert_eq!(Feed::LaunchpadFeed.url(), "wss://launchpad.massive.com");
        assert_eq!(Feed::BusinessFeed.url(), "wss://business.massive.com");
        assert_eq!(Feed::Nasdaq.url(), "wss://nasdaqfeed.massive.com");
    }

    // --- Market ---

    #[test]
    fn test_market_url_path() {
        assert_eq!(Market::Stocks.url_path(), "stocks");
        assert_eq!(Market::Options.url_path(), "options");
        assert_eq!(Market::Forex.url_path(), "forex");
        assert_eq!(Market::Crypto.url_path(), "crypto");
        assert_eq!(Market::Indices.url_path(), "indices");
        assert_eq!(Market::Futures.url_path(), "futures");
        assert_eq!(Market::FuturesCme.url_path(), "futures/cme");
        assert_eq!(Market::FuturesCbot.url_path(), "futures/cbot");
        assert_eq!(Market::FuturesNymex.url_path(), "futures/nymex");
        assert_eq!(Market::FuturesComex.url_path(), "futures/comex");
    }

    /// Port of TestSupportsTopic from Go.
    #[test]
    fn test_supports_topic() {
        // Stocks
        assert!(Market::Stocks.supports(&Topic::StocksMinAggs));
        assert!(Market::Stocks.supports(&Topic::StocksTrades));
        assert!(Market::Stocks.supports(&Topic::StocksImbalances));
        assert!(!Market::Stocks.supports(&Topic::OptionsSecAggs));
        assert!(!Market::Stocks.supports(&Topic::CryptoTrades));

        // Options
        assert!(Market::Options.supports(&Topic::OptionsSecAggs));
        assert!(Market::Options.supports(&Topic::OptionsQuotes));
        assert!(!Market::Options.supports(&Topic::StocksMinAggs));

        // Forex
        assert!(Market::Forex.supports(&Topic::ForexQuotes));
        assert!(Market::Forex.supports(&Topic::ForexSecAggs));
        assert!(!Market::Forex.supports(&Topic::OptionsQuotes));

        // Crypto
        assert!(Market::Crypto.supports(&Topic::CryptoL2Book));
        assert!(Market::Crypto.supports(&Topic::CryptoTrades));
        assert!(!Market::Crypto.supports(&Topic::StocksMinAggs));

        // Indices
        assert!(Market::Indices.supports(&Topic::IndexValue));
        assert!(Market::Indices.supports(&Topic::IndexSecAggs));
        assert!(!Market::Indices.supports(&Topic::StocksTrades));

        // Futures (all sub-markets)
        assert!(Market::Futures.supports(&Topic::FutureTrades));
        assert!(Market::FuturesCme.supports(&Topic::FutureQuotes));
        assert!(Market::FuturesCbot.supports(&Topic::FutureSecAggs));
        assert!(!Market::Futures.supports(&Topic::StocksTrades));
    }

    /// FMV is a cross-market type supported by Stocks, Options, Forex, Crypto.
    #[test]
    fn test_fmv_cross_market_support() {
        assert!(Market::Stocks.supports(&Topic::BusinessFairMarketValue));
        assert!(Market::Options.supports(&Topic::BusinessFairMarketValue));
        assert!(Market::Forex.supports(&Topic::BusinessFairMarketValue));
        assert!(Market::Crypto.supports(&Topic::BusinessFairMarketValue));
        // Not supported for Indices or Futures
        assert!(!Market::Indices.supports(&Topic::BusinessFairMarketValue));
        assert!(!Market::Futures.supports(&Topic::BusinessFairMarketValue));
    }

    // --- Topic ---

    #[test]
    fn test_topic_prefix() {
        assert_eq!(Topic::StocksSecAggs.prefix(), "A");
        assert_eq!(Topic::StocksMinAggs.prefix(), "AM");
        assert_eq!(Topic::StocksTrades.prefix(), "T");
        assert_eq!(Topic::StocksQuotes.prefix(), "Q");
        assert_eq!(Topic::StocksImbalances.prefix(), "NOI");
        assert_eq!(Topic::StocksLULD.prefix(), "LULD");
        assert_eq!(Topic::ForexSecAggs.prefix(), "CAS");
        assert_eq!(Topic::ForexMinAggs.prefix(), "CA");
        assert_eq!(Topic::ForexQuotes.prefix(), "C");
        assert_eq!(Topic::CryptoSecAggs.prefix(), "XAS");
        assert_eq!(Topic::CryptoMinAggs.prefix(), "XA");
        assert_eq!(Topic::CryptoTrades.prefix(), "XT");
        assert_eq!(Topic::CryptoQuotes.prefix(), "XQ");
        assert_eq!(Topic::CryptoL2Book.prefix(), "XL2");
        assert_eq!(Topic::IndexValue.prefix(), "V");
        assert_eq!(Topic::BusinessFairMarketValue.prefix(), "FMV");
        assert_eq!(Topic::FutureTrades.prefix(), "T");
    }

    // --- Config ---

    #[test]
    fn test_config_validate_empty_key() {
        let config = Config {
            api_key: "".into(),
            feed: Feed::RealTime,
            market: Market::Stocks,
            max_retries: None,
            raw_data: false,
            bypass_raw_data_routing: false,
            reconnect_callback: None,
            url_override: None,
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validate_ok() {
        let config = Config {
            api_key: "test".into(),
            feed: Feed::RealTime,
            market: Market::Stocks,
            max_retries: None,
            raw_data: false,
            bypass_raw_data_routing: false,
            reconnect_callback: None,
            url_override: None,
        };
        assert!(config.validate().is_ok());
    }

    /// Port of TestNew URL assertion from Go.
    #[test]
    fn test_config_build_url() {
        let config = Config {
            api_key: "test".into(),
            feed: Feed::PolyFeed,
            market: Market::Options,
            max_retries: None,
            raw_data: false,
            bypass_raw_data_routing: false,
            reconnect_callback: None,
            url_override: None,
        };
        assert_eq!(config.build_url(), "wss://polyfeed.massive.com/options");
    }

    #[test]
    fn test_config_build_url_override() {
        let config = Config {
            api_key: "test".into(),
            feed: Feed::RealTime,
            market: Market::Stocks,
            max_retries: None,
            raw_data: false,
            bypass_raw_data_routing: false,
            reconnect_callback: None,
            url_override: Some("ws://localhost:9999".into()),
        };
        assert_eq!(config.build_url(), "ws://localhost:9999");
    }
}
