use massive_rust_client::websocket::*;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("debug".parse().unwrap()))
        .json()
        .init();

    let api_key = std::env::var("MASSIVE_API_KEY").expect("MASSIVE_API_KEY must be set");

    let (client, mut output, mut errors) = Client::new(Config {
        api_key,
        feed: Feed::RealTime,
        market: Market::Stocks,
        max_retries: None,
        raw_data: false,
        bypass_raw_data_routing: false,
        reconnect_callback: None,
        url_override: None,
    })
    .expect("failed to create client");

    // Subscribe before connecting — messages will be sent after auth.
    client
        .subscribe(Topic::StocksTrades, &["SPY"])
        .await
        .expect("subscribe failed");
    client
        .subscribe(Topic::StocksQuotes, &["SPY"])
        .await
        .expect("subscribe failed");

    client.connect().await.expect("connect failed");

    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);

    loop {
        tokio::select! {
            _ = &mut ctrl_c => break,
            err = errors.recv() => {
                tracing::error!(?err, "fatal error");
                break;
            }
            data = output.recv() => {
                match data {
                    Some(MarketData::EquityAgg(agg)) => {
                        tracing::info!(sym = %agg.symbol, open = agg.open, close = agg.close, "aggregate");
                    }
                    Some(MarketData::EquityTrade(trade)) => {
                        tracing::info!(sym = %trade.symbol, price = trade.price, size = trade.size, "trade");
                    }
                    Some(MarketData::EquityQuote(quote)) => {
                        tracing::info!(sym = %quote.symbol, bid = quote.bid_price, ask = quote.ask_price, "quote");
                    }
                    None => break,
                    _ => {}
                }
            }
        }
    }

    client.close().await;
}
