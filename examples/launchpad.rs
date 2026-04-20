use massive_rust_client::websocket::*;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("debug".parse().unwrap()))
        .json()
        .init();

    let api_key = std::env::var("MASSIVE_API_KEY").expect("MASSIVE_API_KEY must be set");

    // Change Market to match the topics you subscribe to below.
    let (client, mut output, mut errors) = Client::new(Config {
        api_key,
        feed: Feed::LaunchpadFeed,
        market: Market::Stocks,
        max_retries: None,
        raw_data: false,
        bypass_raw_data_routing: false,
        reconnect_callback: None,
        url_override: None,
    })
    .expect("failed to create client");

    // Stocks launchpad
    client
        .subscribe(Topic::StocksLaunchpadMinAggs, &["*"])
        .await
        .expect("subscribe failed");

    // Uncomment for other markets (update Market in Config above):
    // client.subscribe(Topic::OptionsLaunchpadMinAggs, &["O:A230616C00070000"]).await.unwrap();
    // client.subscribe(Topic::ForexLaunchpadMinAggs, &["*"]).await.unwrap();
    // client.subscribe(Topic::CryptoLaunchpadMinAggs, &["*"]).await.unwrap();

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
                    Some(MarketData::CurrencyAgg(agg)) => {
                        tracing::info!(pair = %agg.pair, open = agg.open, close = agg.close, "currency aggregate");
                    }
                    Some(MarketData::EquityAgg(agg)) => {
                        tracing::info!(sym = %agg.symbol, open = agg.open, close = agg.close, "equity aggregate");
                    }
                    Some(MarketData::LaunchpadValue(lv)) => {
                        tracing::info!(sym = %lv.ticker, val = lv.value, "launchpad value");
                    }
                    None => break,
                    _ => {}
                }
            }
        }
    }

    client.close().await;
}
