use std::sync::Arc;
use std::time::Duration;

use backoff::ExponentialBackoff;
use backoff::backoff::Backoff;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tungstenite::protocol::WebSocketConfig;
use tungstenite::protocol::frame::Utf8Bytes;

use crate::websocket::config::{Config, Market, Topic};
use crate::websocket::error::ClientError;
use crate::websocket::models;
use crate::websocket::subscription::{Subscriptions, build_subscribe_message};

const WRITE_WAIT: Duration = Duration::from_secs(5);
const PONG_WAIT: Duration = Duration::from_secs(30);
const PING_PERIOD: Duration = Duration::from_secs(25);
const MAX_MESSAGE_SIZE: usize = 1_000_000;

// Type aliases for the verbose WebSocket types.
type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Tagged enum for all market data types output by the client.
#[derive(Debug, Clone)]
pub enum MarketData {
    EquityAgg(models::EquityAgg),
    EquityTrade(models::EquityTrade),
    EquityQuote(models::EquityQuote),
    Imbalance(models::Imbalance),
    LimitUpLimitDown(models::LimitUpLimitDown),
    CurrencyAgg(models::CurrencyAgg),
    ForexQuote(models::ForexQuote),
    CryptoTrade(models::CryptoTrade),
    CryptoQuote(models::CryptoQuote),
    Level2Book(models::Level2Book),
    IndexValue(models::IndexValue),
    FairMarketValue(models::FairMarketValue),
    LaunchpadValue(models::LaunchpadValue),
    FuturesAggregate(models::FuturesAggregate),
    FuturesTrade(models::FuturesTrade),
    FuturesQuote(models::FuturesQuote),
    SequenceGap(models::SequenceGap),
    Raw(Bytes),
}

/// Client for the Massive WebSocket API.
///
/// Provides real-time streaming of trades, quotes, aggregates, and more
/// across Stocks, Options, Forex, Crypto, Indices, and Futures markets.
pub struct Client {
    inner: Arc<ClientInner>,
}

struct ClientInner {
    api_key: String,
    market: Market,
    url: String,
    raw_data: bool,
    bypass_raw_data_routing: bool,
    max_retries: Option<u64>,

    state: parking_lot::Mutex<ClientState>,

    // Read queue: read task → process task (survives reconnects via cloned sender).
    // Uses Utf8Bytes to preserve the UTF-8 guarantee from tungstenite without re-validation.
    rqueue_tx: mpsc::Sender<Utf8Bytes>,
    // Taken once by connect() to hand to the process task.
    rqueue_rx: tokio::sync::Mutex<Option<mpsc::Receiver<Utf8Bytes>>>,

    // Output channels. Senders behind Option so close() can drop them.
    output_tx: parking_lot::Mutex<Option<mpsc::Sender<MarketData>>>,
    error_tx: parking_lot::Mutex<Option<mpsc::Sender<ClientError>>>,

    // Process task lifecycle (survives reconnects, cancelled on close).
    proc_token: tokio_util::sync::CancellationToken,

    reconnect_callback: Option<Box<dyn Fn(Option<&ClientError>) + Send + Sync>>,
}

struct ClientState {
    connected: bool,
    should_close: bool,
    subs: Subscriptions,
    wqueue_tx: Option<mpsc::Sender<String>>,
    rw_token: Option<tokio_util::sync::CancellationToken>,
}

impl Client {
    /// Creates a new client for the Massive WebSocket API.
    ///
    /// Returns the client along with receivers for market data output and errors.
    pub fn new(
        config: Config,
    ) -> Result<
        (
            Self,
            mpsc::Receiver<MarketData>,
            mpsc::Receiver<ClientError>,
        ),
        ClientError,
    > {
        config.validate()?;

        let url = config.build_url();
        let (rqueue_tx, rqueue_rx) = mpsc::channel(10_000);
        let (output_tx, output_rx) = mpsc::channel(100_000);
        let (error_tx, error_rx) = mpsc::channel(1);

        let inner = Arc::new(ClientInner {
            api_key: config.api_key,
            market: config.market,
            url,
            raw_data: config.raw_data,
            bypass_raw_data_routing: config.bypass_raw_data_routing,
            max_retries: config.max_retries,
            state: parking_lot::Mutex::new(ClientState {
                connected: false,
                should_close: false,
                subs: Subscriptions::new(),
                wqueue_tx: None,
                rw_token: None,
            }),
            rqueue_tx,
            rqueue_rx: tokio::sync::Mutex::new(Some(rqueue_rx)),
            output_tx: parking_lot::Mutex::new(Some(output_tx)),
            error_tx: parking_lot::Mutex::new(Some(error_tx)),
            proc_token: tokio_util::sync::CancellationToken::new(),
            reconnect_callback: config.reconnect_callback,
        });

        Ok((Client { inner }, output_rx, error_rx))
    }

    /// Dials the WebSocket server and starts streaming. Any subscriptions
    /// pushed before connecting will be sent after authentication.
    pub async fn connect(&self) -> Result<(), ClientError> {
        let rqueue_rx = self
            .inner
            .rqueue_rx
            .lock()
            .await
            .take()
            .ok_or_else(|| ClientError::InvalidConfig("already connected".into()))?;

        // Initial connection with backoff.
        do_connect(self.inner.clone(), false).await?;

        // Spawn the process task (lives for the lifetime of the client).
        let output_tx = self
            .inner
            .output_tx
            .lock()
            .as_ref()
            .ok_or_else(|| ClientError::InvalidConfig("client closed".into()))?
            .clone();
        let error_tx = self
            .inner
            .error_tx
            .lock()
            .as_ref()
            .ok_or_else(|| ClientError::InvalidConfig("client closed".into()))?
            .clone();

        let market = self.inner.market;
        let raw_data = self.inner.raw_data;
        let bypass = self.inner.bypass_raw_data_routing;
        let proc_token = self.inner.proc_token.clone();

        tokio::spawn(async move {
            process_loop(
                rqueue_rx, output_tx, error_tx, market, raw_data, bypass, proc_token,
            )
            .await;
        });

        Ok(())
    }

    /// Subscribes to a topic for the given tickers. If no tickers are provided,
    /// subscribes to all tickers for that topic.
    pub async fn subscribe(&self, topic: Topic, tickers: &[&str]) -> Result<(), ClientError> {
        let mut state = self.inner.state.lock();

        if !self.inner.market.supports(&topic) {
            return Err(ClientError::UnsupportedTopic(topic, self.inner.market));
        }

        let tickers: Vec<String> = if tickers.is_empty() || tickers.contains(&"*") {
            vec!["*".to_string()]
        } else {
            tickers.iter().map(|s| s.to_string()).collect()
        };

        let msg = build_subscribe_message("subscribe", topic, &tickers)?;
        state.subs.add(topic, &tickers);

        if let Some(tx) = &state.wqueue_tx {
            let _ = tx.try_send(msg);
        }

        Ok(())
    }

    /// Unsubscribes from a topic for the given tickers. If no tickers are provided,
    /// unsubscribes from all cached tickers for that topic.
    pub async fn unsubscribe(&self, topic: Topic, tickers: &[&str]) -> Result<(), ClientError> {
        let mut state = self.inner.state.lock();

        if !self.inner.market.supports(&topic) {
            return Err(ClientError::UnsupportedTopic(topic, self.inner.market));
        }

        let tickers: Vec<String> = if tickers.is_empty() || tickers.contains(&"*") {
            state.subs.get_tickers(&topic)
        } else {
            tickers.iter().map(|s| s.to_string()).collect()
        };

        let msg = build_subscribe_message("unsubscribe", topic, &tickers)?;
        state.subs.delete(topic, &tickers);

        if let Some(tx) = &state.wqueue_tx {
            let _ = tx.try_send(msg);
        }

        Ok(())
    }

    /// Gracefully closes the connection.
    pub async fn close(&self) {
        {
            let mut state = self.inner.state.lock();
            if state.should_close {
                return;
            }
            state.should_close = true;
            state.wqueue_tx = None;
            if let Some(token) = state.rw_token.take() {
                token.cancel();
            }
        }

        self.inner.proc_token.cancel();

        // Drop output/error senders to close the channels.
        *self.inner.output_tx.lock() = None;
        *self.inner.error_tx.lock() = None;

        tracing::debug!("client closed");
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock();
        state.should_close = true;
        state.wqueue_tx = None;
        if let Some(token) = state.rw_token.take() {
            token.cancel();
        }
        self.inner.proc_token.cancel();
    }
}

// ---------------------------------------------------------------------------
// Connection management
// ---------------------------------------------------------------------------

fn do_connect(
    inner: Arc<ClientInner>,
    reconnect: bool,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ClientError>> + Send>> {
    Box::pin(async move {
        // On reconnect, defensively ensure the old connection state is cleared.
        if reconnect {
            let mut state = inner.state.lock();
            state.wqueue_tx = None;
            if let Some(token) = state.rw_token.take() {
                token.cancel();
            }
            state.connected = false;
        }

        let mut ws_config = WebSocketConfig::default();
        ws_config.max_message_size = Some(MAX_MESSAGE_SIZE);

        let (ws_stream, _) =
            tokio_tungstenite::connect_async_with_config(&inner.url, Some(ws_config), false)
                .await?;

        tracing::debug!("connected to {}", inner.url);

        let (write_half, read_half) = ws_stream.split();

        // New write queue per connection (matches Go behavior).
        let (wqueue_tx, wqueue_rx) = mpsc::channel::<String>(1_000);

        // Send auth message.
        let auth_msg = serde_json::to_string(&serde_json::json!({
            "action": "auth",
            "params": &inner.api_key,
        }))
        .map_err(ClientError::Json)?;
        wqueue_tx
            .send(auth_msg)
            .await
            .map_err(|_| ClientError::ChannelClosed)?;

        // Replay cached subscriptions.
        let sub_msgs = inner.state.lock().subs.get_messages();
        for msg in sub_msgs {
            wqueue_tx
                .send(msg)
                .await
                .map_err(|_| ClientError::ChannelClosed)?;
        }

        let rw_token = tokio_util::sync::CancellationToken::new();
        let pong_received = Arc::new(Notify::new());

        // Spawn read task.
        let rqueue_tx = inner.rqueue_tx.clone();
        let rt = rw_token.clone();
        let pong_tx = pong_received.clone();
        let read_handle = tokio::spawn(read_loop(read_half, rqueue_tx, rt, pong_tx));

        // Spawn write task.
        let wt = rw_token.clone();
        let write_handle = tokio::spawn(write_loop(write_half, wqueue_rx, wt, pong_received));

        // Update shared state.
        {
            let mut state = inner.state.lock();
            state.wqueue_tx = Some(wqueue_tx);
            state.rw_token = Some(rw_token.clone());
            state.connected = true;
        }

        // Spawn connection monitor (detects disconnect, triggers reconnect).
        let monitor_inner = inner.clone();
        tokio::spawn(connection_monitor(
            monitor_inner,
            read_handle,
            write_handle,
            rw_token,
        ));

        Ok(())
    }) // Box::pin
}

/// Watches the read/write tasks; on unexpected exit, triggers reconnection.
async fn connection_monitor(
    inner: Arc<ClientInner>,
    read_handle: JoinHandle<()>,
    write_handle: JoinHandle<()>,
    rw_token: tokio_util::sync::CancellationToken,
) {
    let mut read_handle = read_handle;
    let mut write_handle = write_handle;

    // Wait for either task to exit.
    let read_exited_first = tokio::select! {
        _ = &mut read_handle => {
            tracing::debug!("read task exited");
            true
        }
        _ = &mut write_handle => {
            tracing::debug!("write task exited");
            false
        }
    };

    // Cancel the surviving task.
    rw_token.cancel();

    // Wait for the surviving task to fully shut down. This ensures the old
    // WebSocket (including its TCP socket) is closed before we reconnect,
    // preventing "max_connections" errors on the server.
    let shutdown_timeout = WRITE_WAIT + Duration::from_secs(1);
    if read_exited_first {
        if tokio::time::timeout(shutdown_timeout, &mut write_handle).await.is_err() {
            write_handle.abort();
        }
    } else {
        if tokio::time::timeout(shutdown_timeout, &mut read_handle).await.is_err() {
            read_handle.abort();
        }
    }

    // Clear connection state so the server sees a clean disconnect.
    {
        let mut state = inner.state.lock();
        state.connected = false;
        state.wqueue_tx = None;
    }

    let should_close = inner.state.lock().should_close;
    if should_close {
        return;
    }

    tracing::debug!("unexpected disconnect: reconnecting");

    // Reconnect with exponential backoff.
    let mut backoff = ExponentialBackoff {
        max_elapsed_time: None, // retry until max_retries or forever
        ..Default::default()
    };

    let mut attempts = 0u64;

    loop {
        match do_connect(inner.clone(), true).await {
            Ok(()) => {
                tracing::debug!("reconnection successful");
                if let Some(ref cb) = inner.reconnect_callback {
                    cb(None);
                }
                return;
            }
            Err(e) => {
                attempts += 1;
                tracing::error!("reconnect attempt {attempts} failed: {e}");

                if let Some(ref cb) = inner.reconnect_callback {
                    cb(Some(&e));
                }

                if let Some(max) = inner.max_retries {
                    if attempts >= max {
                        tracing::error!("max reconnection attempts exceeded");
                        close_internal(&inner);
                        if let Some(tx) = inner.error_tx.lock().as_ref() {
                            let _ = tx.try_send(ClientError::MaxRetriesExceeded);
                        }
                        return;
                    }
                }

                match backoff.next_backoff() {
                    Some(duration) => {
                        tracing::debug!("retrying in {duration:?}");
                        tokio::time::sleep(duration).await;
                    }
                    None => {
                        tracing::error!("backoff exhausted");
                        close_internal(&inner);
                        if let Some(tx) = inner.error_tx.lock().as_ref() {
                            let _ = tx.try_send(ClientError::MaxRetriesExceeded);
                        }
                        return;
                    }
                }
            }
        }
    }
}

fn close_internal(inner: &ClientInner) {
    let mut state = inner.state.lock();
    state.should_close = true;
    state.wqueue_tx = None;
    if let Some(token) = state.rw_token.take() {
        token.cancel();
    }
    drop(state);
    inner.proc_token.cancel();
    *inner.output_tx.lock() = None;
    *inner.error_tx.lock() = None;
}

// ---------------------------------------------------------------------------
// Task loops
// ---------------------------------------------------------------------------

async fn read_loop(
    mut read_half: futures_util::stream::SplitStream<WsStream>,
    rqueue_tx: mpsc::Sender<Utf8Bytes>,
    token: tokio_util::sync::CancellationToken,
    pong_notify: Arc<Notify>,
) {
    loop {
        tokio::select! {
            _ = token.cancelled() => break,
            msg = read_half.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        // Utf8Bytes preserves tungstenite's UTF-8 guarantee through the channel.
                        if rqueue_tx.send(text).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Binary(data))) => {
                        // Binary frames are uncommon in this protocol; convert to Utf8Bytes.
                        let text = match Utf8Bytes::try_from(data) {
                            Ok(t) => t,
                            Err(e) => {
                                tracing::error!("binary frame is not valid UTF-8: {e}");
                                continue;
                            }
                        };
                        if rqueue_tx.send(text).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Ping(_))) => {
                        // tungstenite auto-responds to pings
                    }
                    Some(Ok(Message::Pong(_))) => {
                        pong_notify.notify_one();
                    }
                    Some(Ok(Message::Close(_))) => {
                        tracing::debug!("received close frame");
                        break;
                    }
                    Some(Ok(Message::Frame(_))) => {}
                    Some(Err(e)) => {
                        tracing::error!("websocket read error: {e}");
                        break;
                    }
                    None => break,
                }
            }
        }
    }

    // Signal disconnect so the monitor triggers reconnection.
    token.cancel();
    tracing::debug!("read task closed");
}

async fn write_loop(
    mut write_half: futures_util::stream::SplitSink<WsStream, Message>,
    mut wqueue_rx: mpsc::Receiver<String>,
    token: tokio_util::sync::CancellationToken,
    pong_notify: Arc<Notify>,
) {
    let mut ping_interval = tokio::time::interval(PING_PERIOD);
    ping_interval.tick().await; // skip the immediate first tick

    // Pong deadline: armed after each ping, disarmed on pong receipt.
    let pong_deadline = tokio::time::sleep(PONG_WAIT);
    tokio::pin!(pong_deadline);
    let mut pong_deadline_active = false;

    loop {
        tokio::select! {
            _ = token.cancelled() => {
                // Attempt a graceful close frame (best-effort).
                let _ = tokio::time::timeout(WRITE_WAIT, write_half.send(
                    Message::Close(Some(CloseFrame {
                        code: CloseCode::Normal,
                        reason: "".into(),
                    }))
                )).await;
                break;
            }
            _ = &mut pong_deadline, if pong_deadline_active => {
                tracing::error!("pong deadline exceeded; disconnecting");
                break;
            }
            _ = pong_notify.notified() => {
                // Pong received — disarm the deadline.
                pong_deadline_active = false;
            }
            _ = ping_interval.tick() => {
                let res = tokio::time::timeout(
                    WRITE_WAIT,
                    write_half.send(Message::Ping(Vec::new().into())),
                ).await;
                if res.is_err() || res.unwrap().is_err() {
                    tracing::error!("failed to send ping");
                    break;
                }
                // Arm the pong deadline.
                pong_deadline.as_mut().reset(tokio::time::Instant::now() + PONG_WAIT);
                pong_deadline_active = true;
            }
            msg = wqueue_rx.recv() => {
                match msg {
                    Some(data) => {
                        let res = tokio::time::timeout(
                            WRITE_WAIT,
                            write_half.send(Message::Text(data.into())),
                        ).await;
                        if res.is_err() || res.unwrap().is_err() {
                            tracing::error!("failed to send message");
                            break;
                        }
                    }
                    None => break,
                }
            }
        }
    }

    token.cancel();
    tracing::debug!("write task closed");
}

// ---------------------------------------------------------------------------
// Sequence gap detection
// ---------------------------------------------------------------------------

use compact_str::CompactString;

/// Tracks the last sequence number per (symbol, event_type) pair.
///
/// Uses a Vec with linear scan instead of HashMap: no hashing, no key cloning
/// on lookup, and cache-friendly for the typical <100 ticker working set.
struct SequenceTracker {
    entries: Vec<(CompactString, CompactString, i64)>,
}

impl SequenceTracker {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Checks for a sequence discontinuity. Returns `Some(SequenceGap)` when
    /// `received != last_seen + 1` for that (symbol, event_type) pair.
    /// Skips messages with sequence_number == 0 (field absent from JSON).
    fn check(&mut self, md: &MarketData) -> Option<models::SequenceGap> {
        let (symbol, ev, seq) = Self::extract(md)?;
        if seq == 0 {
            return None;
        }

        // Linear scan — compare borrowed references, no cloning on the hot path.
        for entry in self.entries.iter_mut() {
            if entry.0 == *symbol && entry.1 == *ev {
                let prev = entry.2;
                entry.2 = seq;
                if seq != prev + 1 {
                    return Some(models::SequenceGap {
                        symbol: symbol.clone(),
                        event_type: ev.clone(),
                        last_seen: prev,
                        received: seq,
                    });
                }
                return None;
            }
        }

        // First occurrence for this (symbol, event_type) — record it.
        self.entries.push((symbol.clone(), ev.clone(), seq));
        None
    }

    fn clear(&mut self) {
        self.entries.clear();
    }

    fn extract(md: &MarketData) -> Option<(&CompactString, &CompactString, i64)> {
        match md {
            MarketData::EquityTrade(t) => Some((&t.symbol, &t.event_type, t.sequence_number)),
            MarketData::EquityQuote(q) => Some((&q.symbol, &q.event_type, q.sequence_number)),
            MarketData::LimitUpLimitDown(l) => Some((&l.symbol, &l.event_type, l.sequence_number)),
            MarketData::FuturesTrade(t) => Some((&t.symbol, &t.event_type, t.sequence_number)),
            _ => None,
        }
    }
}

async fn process_loop(
    mut rqueue_rx: mpsc::Receiver<Utf8Bytes>,
    output_tx: mpsc::Sender<MarketData>,
    error_tx: mpsc::Sender<ClientError>,
    market: Market,
    raw_data: bool,
    bypass_raw_data_routing: bool,
    proc_token: tokio_util::sync::CancellationToken,
) {
    let mut seq_tracker = SequenceTracker::new();

    loop {
        tokio::select! {
            _ = proc_token.cancelled() => break,
            msg = rqueue_rx.recv() => {
                let Some(data) = msg else { break };

                if raw_data && bypass_raw_data_routing {
                    let _ = output_tx.send(MarketData::Raw(Bytes::from(data))).await;
                    continue;
                }

                // Utf8Bytes guarantees valid UTF-8 (validated by tungstenite on receipt).
                let str_data: &str = &data;

                // Zero-alloc iteration over JSON array elements.
                for raw_element in json_array_elements(str_data) {
                    let event_type = match peek_event_type(raw_element.as_bytes()) {
                        Some(ev) => ev,
                        None => {
                            tracing::error!("failed to extract event type from message");
                            continue;
                        }
                    };

                    match event_type {
                        "status" => {
                            // Server sends "connected" on each new connection.
                            // Clear the tracker so a reconnect doesn't false-positive.
                            if raw_element.contains("\"connected\"") {
                                seq_tracker.clear();
                            }
                            if let Err(e) = handle_status(raw_element) {
                                let _ = error_tx.send(e).await;
                                return; // fatal
                            }
                        }
                        _ => {
                            if raw_data {
                                let _ = output_tx
                                    .send(MarketData::Raw(Bytes::copy_from_slice(
                                        raw_element.as_bytes(),
                                    )))
                                    .await;
                            } else {
                                match route_data(&market, event_type, raw_element) {
                                    Ok(Some(market_data)) => {
                                        if let Some(gap) = seq_tracker.check(&market_data) {
                                            tracing::warn!(
                                                symbol = %gap.symbol,
                                                event_type = %gap.event_type,
                                                last_seen = gap.last_seen,
                                                received = gap.received,
                                                "sequence gap detected"
                                            );
                                            let _ = output_tx.send(MarketData::SequenceGap(gap)).await;
                                        }
                                        let _ = output_tx.send(market_data).await;
                                    }
                                    Ok(None) => {}
                                    Err(e) => {
                                        tracing::error!("deserialization error: {e}");
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    tracing::debug!("process task closed");
}

/// Extracts the `"ev"` field value from raw JSON bytes using a fast byte scan.
/// Avoids a full serde parse — ~227x faster than the serde EventType approach.
pub fn peek_event_type(raw: &[u8]) -> Option<&str> {
    let needle = b"\"ev\":\"";
    let pos = memchr::memmem::find(raw, needle)?;
    let start = pos + needle.len();
    let end = start + memchr::memchr(b'"', &raw[start..])?;
    std::str::from_utf8(&raw[start..end]).ok()
}

/// Zero-allocation iterator over elements in a JSON array string.
///
/// Scans for object boundaries by tracking brace depth, correctly handling
/// strings with escaped characters. Yields `&str` slices of each element.
/// Avoids the `Vec<&RawValue>` allocation entirely.
pub fn json_array_elements(input: &str) -> JsonArrayIter<'_> {
    let data = input.as_bytes();
    // Skip past the opening '[' of the outer array.
    let start = memchr::memchr(b'[', data).map(|p| p + 1).unwrap_or(0);
    JsonArrayIter { data, pos: start }
}

pub struct JsonArrayIter<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Iterator for JsonArrayIter<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        let data = self.data;
        let len = data.len();

        // Skip whitespace/commas to the start of the next element.
        while self.pos < len {
            match data[self.pos] {
                b'{' => break,
                b']' => return None, // end of array
                _ => self.pos += 1,
            }
        }

        if self.pos >= len {
            return None;
        }

        let start = self.pos;
        let mut depth: u32 = 0;
        let mut in_string = false;

        while self.pos < len {
            let b = data[self.pos];
            if in_string {
                if b == b'\\' {
                    self.pos += 1; // skip escaped character
                } else if b == b'"' {
                    in_string = false;
                }
            } else {
                match b {
                    b'"' => in_string = true,
                    b'{' | b'[' => depth += 1,
                    b'}' | b']' => {
                        depth -= 1;
                        if depth == 0 {
                            self.pos += 1;
                            // SAFETY: input was &str so all byte boundaries are valid UTF-8.
                            let element =
                                unsafe { std::str::from_utf8_unchecked(&data[start..self.pos]) };
                            return Some(element);
                        }
                    }
                    _ => {}
                }
            }
            self.pos += 1;
        }

        None
    }
}

// ---------------------------------------------------------------------------
// Message routing
// ---------------------------------------------------------------------------

fn handle_status(raw: &str) -> Result<(), ClientError> {
    let cm: models::ControlMessage = match serde_json::from_str(raw) {
        Ok(m) => m,
        Err(e) => {
            tracing::error!("failed to unmarshal status message: {e}");
            return Ok(());
        }
    };

    match cm.status.as_str() {
        "connected" => tracing::debug!("connection successful"),
        "auth_success" => tracing::debug!("authentication successful"),
        "auth_failed" => {
            return Err(ClientError::AuthFailed(
                "authentication failed: closing connection".into(),
            ));
        }
        "max_connections" => {
            return Err(ClientError::MaxConnections(cm.message.to_string()));
        }
        "success" => tracing::debug!("status success: {}", sanitize(&cm.message)),
        "error" => tracing::error!("status error: {}", sanitize(&cm.message)),
        other => tracing::info!(
            "unknown status '{}': {}",
            sanitize(other),
            sanitize(&cm.message)
        ),
    }

    Ok(())
}

fn route_data(
    market: &Market,
    event_type: &str,
    raw: &str,
) -> Result<Option<MarketData>, serde_json::Error> {
    macro_rules! deser {
        ($variant:ident) => {{
            let v = serde_json::from_str(raw)?;
            Ok(Some(MarketData::$variant(v)))
        }};
    }

    // Cross-market types checked first.
    match event_type {
        "FMV" => return deser!(FairMarketValue),
        "LV" => return deser!(LaunchpadValue),
        _ => {}
    }

    match market {
        Market::Stocks => match event_type {
            "A" | "AM" => deser!(EquityAgg),
            "T" => deser!(EquityTrade),
            "Q" => deser!(EquityQuote),
            "LULD" => deser!(LimitUpLimitDown),
            "NOI" => deser!(Imbalance),
            _ => {
                tracing::info!("unknown message type '{event_type}' for market {market}");
                Ok(None)
            }
        },
        Market::Options => match event_type {
            "A" | "AM" => deser!(EquityAgg),
            "T" => deser!(EquityTrade),
            "Q" => deser!(EquityQuote),
            _ => {
                tracing::info!("unknown message type '{event_type}' for market {market}");
                Ok(None)
            }
        },
        Market::Forex => match event_type {
            "CA" | "CAS" => deser!(CurrencyAgg),
            "C" => deser!(ForexQuote),
            _ => {
                tracing::info!("unknown message type '{event_type}' for market {market}");
                Ok(None)
            }
        },
        Market::Crypto => match event_type {
            "XA" | "XAS" => deser!(CurrencyAgg),
            "XT" => deser!(CryptoTrade),
            "XQ" => deser!(CryptoQuote),
            "XL2" => deser!(Level2Book),
            _ => {
                tracing::info!("unknown message type '{event_type}' for market {market}");
                Ok(None)
            }
        },
        Market::Indices => match event_type {
            "A" | "AM" => deser!(EquityAgg),
            "V" => deser!(IndexValue),
            _ => {
                tracing::info!("unknown message type '{event_type}' for market {market}");
                Ok(None)
            }
        },
        Market::Futures
        | Market::FuturesCme
        | Market::FuturesCbot
        | Market::FuturesNymex
        | Market::FuturesComex => match event_type {
            "A" | "AM" => deser!(FuturesAggregate),
            "T" => deser!(FuturesTrade),
            "Q" => deser!(FuturesQuote),
            _ => {
                tracing::info!("unknown message type '{event_type}' for market {market}");
                Ok(None)
            }
        },
    }
}

fn sanitize(s: &str) -> String {
    s.replace('\n', "")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::websocket::config::{Config, Feed};
    use futures_util::SinkExt;
    use tokio::net::TcpListener;

    /// Spins up a mock WebSocket server that echoes back auth success/failure.
    /// Matches the Go test server in client_test.go.
    async fn mock_ws_server() -> (String, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("ws://127.0.0.1:{}", addr.port());

        let handle = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let ws = match tokio_tungstenite::accept_async(stream).await {
                        Ok(ws) => ws,
                        Err(_) => return,
                    };
                    let (mut write, mut read) = ws.split();

                    while let Some(Ok(msg)) = read.next().await {
                        if let Message::Text(text) = msg {
                            let cm: serde_json::Value = match serde_json::from_str(text.as_ref()) {
                                Ok(v) => v,
                                Err(_) => continue,
                            };

                            if cm["action"] == "auth" && cm["params"] == "good" {
                                let res = serde_json::json!([
                                    {"ev": "status", "status": "auth_success"}
                                ]);
                                let _ = write.send(Message::Text(res.to_string().into())).await;
                            } else if cm["action"] == "auth" {
                                let res = serde_json::json!([
                                    {"ev": "status", "status": "auth_failed"}
                                ]);
                                let _ = write.send(Message::Text(res.to_string().into())).await;
                            } else if cm["action"] == "subscribe" {
                                let res = serde_json::json!([
                                    {"ev": "status", "status": "success", "message": "subscribed"}
                                ]);
                                let _ = write.send(Message::Text(res.to_string().into())).await;
                            }
                        }
                    }
                });
            }
        });

        (url, handle)
    }

    fn test_config(api_key: &str, url: String) -> Config {
        Config {
            api_key: api_key.into(),
            feed: Feed::RealTime,
            market: Market::Stocks,
            max_retries: Some(0),
            raw_data: false,
            bypass_raw_data_routing: false,
            reconnect_callback: None,
            url_override: Some(url),
        }
    }

    // --- Port of TestNew ---

    #[test]
    fn test_new_success() {
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
        let (client, _output, _errors) = Client::new(config).unwrap();
        assert_eq!(client.inner.url, "wss://polyfeed.massive.com/options");
    }

    #[test]
    fn test_new_empty_config() {
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
        assert!(Client::new(config).is_err());
    }

    // --- Port of TestConnectAuthSuccess ---

    #[tokio::test]
    async fn test_connect_auth_success() {
        let (url, _server) = mock_ws_server().await;
        let config = test_config("good", url);
        let (client, _output, _errors) = Client::new(config).unwrap();

        // Close before connect should be fine.
        client.close().await;

        // Create a fresh client to actually connect.
        let (url2, _server2) = mock_ws_server().await;
        let config2 = test_config("good", url2);
        let (client2, _output2, _errors2) = Client::new(config2).unwrap();

        let result = client2.connect().await;
        assert!(result.is_ok());

        // Give auth response time to propagate.
        tokio::time::sleep(Duration::from_millis(100)).await;

        client2.close().await;
    }

    // --- Port of TestConnectAuthFailure ---

    #[tokio::test]
    async fn test_connect_auth_failure() {
        let (url, _server) = mock_ws_server().await;
        let config = test_config("bad", url);
        let (client, _output, mut errors) = Client::new(config).unwrap();

        let result = client.connect().await;
        assert!(result.is_ok()); // connect itself succeeds; auth fail arrives async

        // Should receive an auth error on the error channel.
        let err = tokio::time::timeout(Duration::from_secs(2), errors.recv()).await;
        assert!(err.is_ok());
        let err = err.unwrap().unwrap();
        assert!(matches!(err, ClientError::AuthFailed(_)));

        client.close().await;
    }

    // --- Port of TestConnectRetryFailure ---

    #[tokio::test]
    async fn test_connect_retry_failure() {
        // Connect to a wss:// URL against a plain ws server — should fail.
        let config = Config {
            api_key: "test".into(),
            feed: Feed::RealTime,
            market: Market::Stocks,
            max_retries: Some(1),
            raw_data: false,
            bypass_raw_data_routing: false,
            reconnect_callback: None,
            url_override: Some("wss://127.0.0.1:1".into()), // bad address
        };
        let (client, _output, _errors) = Client::new(config).unwrap();
        let result = client.connect().await;
        assert!(result.is_err());
        client.close().await;
    }

    // --- Port of TestReconnectCallback ---

    #[tokio::test]
    async fn test_reconnect_callback() {
        use std::sync::Arc as StdArc;
        use std::sync::atomic::{AtomicU32, Ordering};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("ws://127.0.0.1:{}", addr.port());

        // Server that accepts multiple connections and always auth-succeeds.
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let Ok(ws) = tokio_tungstenite::accept_async(stream).await else {
                        return;
                    };
                    let (mut write, mut read) = ws.split();
                    while let Some(Ok(msg)) = read.next().await {
                        if let Message::Text(text) = msg {
                            let cm: serde_json::Value = match serde_json::from_str(text.as_ref()) {
                                Ok(v) => v,
                                Err(_) => continue,
                            };
                            if cm["action"] == "auth" && cm["params"] == "good" {
                                let res = serde_json::json!([
                                    {"ev": "status", "status": "auth_success"}
                                ]);
                                let _ = write.send(Message::Text(res.to_string().into())).await;
                            }
                        }
                    }
                });
            }
        });

        let callback_count = StdArc::new(AtomicU32::new(0));
        let cb_count = callback_count.clone();

        let config = Config {
            api_key: "good".into(),
            feed: Feed::RealTime,
            market: Market::Stocks,
            max_retries: Some(2),
            raw_data: false,
            bypass_raw_data_routing: false,
            reconnect_callback: Some(Box::new(move |err| {
                // On successful reconnect, err is None.
                assert!(err.is_none());
                cb_count.fetch_add(1, Ordering::SeqCst);
            })),
            url_override: Some(url),
        };

        let (client, _output, _errors) = Client::new(config).unwrap();
        client.connect().await.unwrap();

        // Force a disconnect by cancelling the rw_token, which triggers reconnection.
        {
            let state = client.inner.state.lock();
            if let Some(ref token) = state.rw_token {
                token.cancel();
            }
        }

        // Wait for reconnection to complete.
        tokio::time::sleep(Duration::from_millis(500)).await;

        assert_eq!(callback_count.load(Ordering::SeqCst), 1);

        client.close().await;
    }

    // --- Connect twice should not error (port of TestConnectAuthSuccess detail) ---

    #[tokio::test]
    async fn test_connect_twice_returns_error() {
        let (url, _server) = mock_ws_server().await;
        let config = test_config("good", url);
        let (client, _output, _errors) = Client::new(config).unwrap();

        let first = client.connect().await;
        assert!(first.is_ok());

        // Second connect should fail because the rqueue_rx was already taken.
        let second = client.connect().await;
        assert!(second.is_err());

        client.close().await;
    }

    // --- Close before connect should not panic ---

    #[tokio::test]
    async fn test_close_before_connect() {
        let (url, _server) = mock_ws_server().await;
        let config = test_config("good", url);
        let (client, _output, _errors) = Client::new(config).unwrap();

        // Should not panic.
        client.close().await;
    }

    // --- Subscribe before connect ---

    #[tokio::test]
    async fn test_subscribe_before_connect() {
        let (url, _server) = mock_ws_server().await;
        let config = test_config("good", url);
        let (client, _output, _errors) = Client::new(config).unwrap();

        // Subscribe before connect — should be cached.
        client
            .subscribe(Topic::StocksTrades, &["SPY"])
            .await
            .unwrap();
        client
            .subscribe(Topic::StocksQuotes, &["SPY"])
            .await
            .unwrap();

        let result = client.connect().await;
        assert!(result.is_ok());

        tokio::time::sleep(Duration::from_millis(100)).await;
        client.close().await;
    }

    // --- Unsupported topic ---

    #[tokio::test]
    async fn test_subscribe_unsupported_topic() {
        let (url, _server) = mock_ws_server().await;
        let config = test_config("good", url);
        let (client, _output, _errors) = Client::new(config).unwrap();

        let result = client.subscribe(Topic::CryptoTrades, &["BTC-USD"]).await;
        assert!(matches!(result, Err(ClientError::UnsupportedTopic(_, _))));

        client.close().await;
    }

    // --- Data routing ---

    #[tokio::test]
    async fn test_data_routing() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("ws://127.0.0.1:{}", addr.port());

        // Server that sends auth_success then a trade message.
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            let (mut write, mut read) = ws.split();

            // Wait for auth.
            if let Some(Ok(_)) = read.next().await {
                let auth_ok = serde_json::json!([{"ev": "status", "status": "auth_success"}]);
                write
                    .send(Message::Text(auth_ok.to_string().into()))
                    .await
                    .unwrap();
            }

            // Wait for subscribe.
            if let Some(Ok(_)) = read.next().await {
                let sub_ok = serde_json::json!([
                    {"ev": "status", "status": "success", "message": "subscribed"}
                ]);
                write
                    .send(Message::Text(sub_ok.to_string().into()))
                    .await
                    .unwrap();

                // Send a trade.
                let trade = serde_json::json!([
                    {"ev":"T","sym":"SPY","p":450.25,"s":100,"t":1_700_000_000_000_i64,"q":1}
                ]);
                write
                    .send(Message::Text(trade.to_string().into()))
                    .await
                    .unwrap();
            }

            // Keep connection alive briefly.
            tokio::time::sleep(Duration::from_secs(1)).await;
        });

        let config = test_config("good", url);
        let (client, mut output, _errors) = Client::new(config).unwrap();

        client
            .subscribe(Topic::StocksTrades, &["SPY"])
            .await
            .unwrap();
        client.connect().await.unwrap();

        // Should receive the trade on the output channel.
        let data = tokio::time::timeout(Duration::from_secs(2), output.recv()).await;
        assert!(data.is_ok());
        let data = data.unwrap().unwrap();
        match data {
            MarketData::EquityTrade(t) => {
                assert_eq!(t.symbol, "SPY");
                assert_eq!(t.price, 450.25);
            }
            other => panic!("expected EquityTrade, got {other:?}"),
        }

        client.close().await;
    }

    // --- Data routing: crypto market ---

    #[tokio::test]
    async fn test_data_routing_crypto() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("ws://127.0.0.1:{}", addr.port());

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            let (mut write, mut read) = ws.split();

            // Auth.
            if let Some(Ok(_)) = read.next().await {
                let res = serde_json::json!([{"ev": "status", "status": "auth_success"}]);
                write
                    .send(Message::Text(res.to_string().into()))
                    .await
                    .unwrap();
            }
            // Subscribe.
            if let Some(Ok(_)) = read.next().await {
                let res = serde_json::json!([{"ev": "status", "status": "success"}]);
                write
                    .send(Message::Text(res.to_string().into()))
                    .await
                    .unwrap();

                let trade = serde_json::json!([
                    {"ev":"XT","pair":"BTC-USD","p":65000.0,"s":0.5,"t":1_700_000_000_000_i64}
                ]);
                write
                    .send(Message::Text(trade.to_string().into()))
                    .await
                    .unwrap();
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        });

        let config = Config {
            api_key: "good".into(),
            feed: Feed::RealTime,
            market: Market::Crypto,
            max_retries: Some(0),
            raw_data: false,
            bypass_raw_data_routing: false,
            reconnect_callback: None,
            url_override: Some(url),
        };
        let (client, mut output, _errors) = Client::new(config).unwrap();

        client
            .subscribe(Topic::CryptoTrades, &["BTC-USD"])
            .await
            .unwrap();
        client.connect().await.unwrap();

        let data = tokio::time::timeout(Duration::from_secs(2), output.recv()).await;
        assert!(data.is_ok());
        match data.unwrap().unwrap() {
            MarketData::CryptoTrade(t) => {
                assert_eq!(t.pair, "BTC-USD");
                assert_eq!(t.price, 65000.0);
            }
            other => panic!("expected CryptoTrade, got {other:?}"),
        }

        client.close().await;
    }

    // --- Data routing: FMV cross-market ---

    #[tokio::test]
    async fn test_data_routing_fmv() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("ws://127.0.0.1:{}", addr.port());

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            let (mut write, mut read) = ws.split();

            if let Some(Ok(_)) = read.next().await {
                let res = serde_json::json!([{"ev": "status", "status": "auth_success"}]);
                write
                    .send(Message::Text(res.to_string().into()))
                    .await
                    .unwrap();
            }
            if let Some(Ok(_)) = read.next().await {
                let res = serde_json::json!([{"ev": "status", "status": "success"}]);
                write
                    .send(Message::Text(res.to_string().into()))
                    .await
                    .unwrap();

                let fmv = serde_json::json!([
                    {"ev":"FMV","fmv":155.42,"sym":"AAPL","t":1_700_000_000_000_i64}
                ]);
                write
                    .send(Message::Text(fmv.to_string().into()))
                    .await
                    .unwrap();
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        });

        let config = Config {
            api_key: "good".into(),
            feed: Feed::BusinessFeed,
            market: Market::Stocks,
            max_retries: Some(0),
            raw_data: false,
            bypass_raw_data_routing: false,
            reconnect_callback: None,
            url_override: Some(url),
        };
        let (client, mut output, _errors) = Client::new(config).unwrap();

        client
            .subscribe(Topic::BusinessFairMarketValue, &["*"])
            .await
            .unwrap();
        client.connect().await.unwrap();

        let data = tokio::time::timeout(Duration::from_secs(2), output.recv()).await;
        assert!(data.is_ok());
        match data.unwrap().unwrap() {
            MarketData::FairMarketValue(fmv) => {
                assert_eq!(fmv.ticker, "AAPL");
                assert_eq!(fmv.fmv, 155.42);
            }
            other => panic!("expected FairMarketValue, got {other:?}"),
        }

        client.close().await;
    }

    // --- Raw data mode ---

    #[tokio::test]
    async fn test_raw_data_mode() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("ws://127.0.0.1:{}", addr.port());

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            let (mut write, mut read) = ws.split();

            if let Some(Ok(_)) = read.next().await {
                let res = serde_json::json!([{"ev": "status", "status": "auth_success"}]);
                write
                    .send(Message::Text(res.to_string().into()))
                    .await
                    .unwrap();
            }
            if let Some(Ok(_)) = read.next().await {
                let res = serde_json::json!([{"ev": "status", "status": "success"}]);
                write
                    .send(Message::Text(res.to_string().into()))
                    .await
                    .unwrap();

                let trade = serde_json::json!([
                    {"ev":"T","sym":"SPY","p":450.0,"s":100,"t":1_700_000_000_000_i64}
                ]);
                write
                    .send(Message::Text(trade.to_string().into()))
                    .await
                    .unwrap();
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        });

        let config = Config {
            api_key: "good".into(),
            feed: Feed::RealTime,
            market: Market::Stocks,
            max_retries: Some(0),
            raw_data: true,
            bypass_raw_data_routing: false,
            reconnect_callback: None,
            url_override: Some(url),
        };
        let (client, mut output, _errors) = Client::new(config).unwrap();

        client
            .subscribe(Topic::StocksTrades, &["SPY"])
            .await
            .unwrap();
        client.connect().await.unwrap();

        let data = tokio::time::timeout(Duration::from_secs(2), output.recv()).await;
        assert!(data.is_ok());
        match data.unwrap().unwrap() {
            MarketData::Raw(bytes) => {
                // Raw mode returns the individual JSON element (not the array).
                let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(v["ev"], "T");
                assert_eq!(v["sym"], "SPY");
            }
            other => panic!("expected Raw, got {other:?}"),
        }

        client.close().await;
    }

    // --- Raw data bypass mode ---

    #[tokio::test]
    async fn test_raw_data_bypass_mode() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("ws://127.0.0.1:{}", addr.port());

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            let (mut write, mut read) = ws.split();

            // Don't even wait for auth — bypass means caller handles everything.
            if let Some(Ok(_)) = read.next().await {
                let msg = serde_json::json!([{"ev":"status","status":"auth_success"}]);
                write
                    .send(Message::Text(msg.to_string().into()))
                    .await
                    .unwrap();

                let data = serde_json::json!([{"ev":"T","sym":"AAPL","p":150.0}]);
                write
                    .send(Message::Text(data.to_string().into()))
                    .await
                    .unwrap();
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        });

        let config = Config {
            api_key: "good".into(),
            feed: Feed::RealTime,
            market: Market::Stocks,
            max_retries: Some(0),
            raw_data: true,
            bypass_raw_data_routing: true,
            reconnect_callback: None,
            url_override: Some(url),
        };
        let (client, mut output, _errors) = Client::new(config).unwrap();

        client.connect().await.unwrap();

        // In bypass mode, raw bytes come through without any internal routing.
        let data = tokio::time::timeout(Duration::from_secs(2), output.recv()).await;
        assert!(data.is_ok());
        match data.unwrap().unwrap() {
            MarketData::Raw(bytes) => {
                // Should be the full array, not individual elements.
                let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                assert!(v.is_array());
            }
            other => panic!("expected Raw, got {other:?}"),
        }

        client.close().await;
    }

    // --- Unsubscribe ---

    #[tokio::test]
    async fn test_unsubscribe() {
        let (url, _server) = mock_ws_server().await;
        let config = test_config("good", url);
        let (client, _output, _errors) = Client::new(config).unwrap();

        client
            .subscribe(Topic::StocksTrades, &["AAPL", "TSLA"])
            .await
            .unwrap();
        client
            .unsubscribe(Topic::StocksTrades, &["AAPL"])
            .await
            .unwrap();

        // Verify internal state: only TSLA should remain.
        let state = client.inner.state.lock();
        let tickers = state.subs.get_tickers(&Topic::StocksTrades);
        assert!(tickers.contains(&"TSLA".to_string()));
        assert!(!tickers.contains(&"AAPL".to_string()));
    }

    // --- Unsubscribe all (no tickers) ---

    #[tokio::test]
    async fn test_unsubscribe_all() {
        let (url, _server) = mock_ws_server().await;
        let config = test_config("good", url);
        let (client, _output, _errors) = Client::new(config).unwrap();

        client
            .subscribe(Topic::StocksTrades, &["AAPL", "TSLA"])
            .await
            .unwrap();
        client.unsubscribe(Topic::StocksTrades, &[]).await.unwrap();

        let state = client.inner.state.lock();
        let tickers = state.subs.get_tickers(&Topic::StocksTrades);
        assert!(tickers.is_empty());
    }

    // --- Multiple messages in one array ---

    #[tokio::test]
    async fn test_multiple_messages_in_array() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("ws://127.0.0.1:{}", addr.port());

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            let (mut write, mut read) = ws.split();

            if let Some(Ok(_)) = read.next().await {
                let res = serde_json::json!([{"ev": "status", "status": "auth_success"}]);
                write
                    .send(Message::Text(res.to_string().into()))
                    .await
                    .unwrap();
            }
            if let Some(Ok(_)) = read.next().await {
                let res = serde_json::json!([{"ev": "status", "status": "success"}]);
                write
                    .send(Message::Text(res.to_string().into()))
                    .await
                    .unwrap();

                // Send 3 trades in one array (server batching).
                let batch = serde_json::json!([
                    {"ev":"T","sym":"AAPL","p":150.0,"s":10,"t":1_700_000_000_000_i64,"q":1},
                    {"ev":"T","sym":"TSLA","p":250.0,"s":20,"t":1_700_000_000_001_i64,"q":2},
                    {"ev":"T","sym":"MSFT","p":350.0,"s":30,"t":1_700_000_000_002_i64,"q":3}
                ]);
                write
                    .send(Message::Text(batch.to_string().into()))
                    .await
                    .unwrap();
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        });

        let config = test_config("good", url);
        let (client, mut output, _errors) = Client::new(config).unwrap();

        client.subscribe(Topic::StocksTrades, &["*"]).await.unwrap();
        client.connect().await.unwrap();

        // Should receive 3 separate MarketData items.
        let mut symbols = Vec::new();
        for _ in 0..3 {
            let data = tokio::time::timeout(Duration::from_secs(2), output.recv()).await;
            match data.unwrap().unwrap() {
                MarketData::EquityTrade(t) => symbols.push(t.symbol.clone()),
                other => panic!("expected EquityTrade, got {other:?}"),
            }
        }

        symbols.sort();
        assert_eq!(symbols, vec!["AAPL", "MSFT", "TSLA"]);

        client.close().await;
    }

    // --- Sequence gap detection ---

    #[test]
    fn test_sequence_tracker_no_gap() {
        let mut tracker = SequenceTracker::new();

        let t1 = MarketData::EquityTrade(models::EquityTrade {
            event_type: "T".into(),
            symbol: "AAPL".into(),
            sequence_number: 1,
            ..Default::default()
        });
        let t2 = MarketData::EquityTrade(models::EquityTrade {
            event_type: "T".into(),
            symbol: "AAPL".into(),
            sequence_number: 2,
            ..Default::default()
        });

        assert!(tracker.check(&t1).is_none()); // first seen
        assert!(tracker.check(&t2).is_none()); // consecutive
    }

    #[test]
    fn test_sequence_tracker_forward_gap() {
        let mut tracker = SequenceTracker::new();

        let t1 = MarketData::EquityTrade(models::EquityTrade {
            event_type: "T".into(),
            symbol: "SPY".into(),
            sequence_number: 10,
            ..Default::default()
        });
        let t2 = MarketData::EquityTrade(models::EquityTrade {
            event_type: "T".into(),
            symbol: "SPY".into(),
            sequence_number: 15,
            ..Default::default()
        });

        assert!(tracker.check(&t1).is_none());
        let gap = tracker.check(&t2).expect("should detect gap");
        assert_eq!(gap.symbol, "SPY");
        assert_eq!(gap.event_type, "T");
        assert_eq!(gap.last_seen, 10);
        assert_eq!(gap.received, 15);
    }

    #[test]
    fn test_sequence_tracker_backward_jump() {
        let mut tracker = SequenceTracker::new();

        let t1 = MarketData::EquityQuote(models::EquityQuote {
            event_type: "Q".into(),
            symbol: "TSLA".into(),
            sequence_number: 100,
            ..Default::default()
        });
        let t2 = MarketData::EquityQuote(models::EquityQuote {
            event_type: "Q".into(),
            symbol: "TSLA".into(),
            sequence_number: 50,
            ..Default::default()
        });

        assert!(tracker.check(&t1).is_none());
        let gap = tracker.check(&t2).expect("should detect backward jump");
        assert_eq!(gap.last_seen, 100);
        assert_eq!(gap.received, 50);
    }

    #[test]
    fn test_sequence_tracker_independent_streams() {
        let mut tracker = SequenceTracker::new();

        // Trade and quote for same ticker have independent sequence spaces.
        let trade = MarketData::EquityTrade(models::EquityTrade {
            event_type: "T".into(),
            symbol: "AAPL".into(),
            sequence_number: 1,
            ..Default::default()
        });
        let quote = MarketData::EquityQuote(models::EquityQuote {
            event_type: "Q".into(),
            symbol: "AAPL".into(),
            sequence_number: 1,
            ..Default::default()
        });

        assert!(tracker.check(&trade).is_none());
        assert!(tracker.check(&quote).is_none()); // different stream, no gap
    }

    #[test]
    fn test_sequence_tracker_skips_zero() {
        let mut tracker = SequenceTracker::new();

        let t = MarketData::EquityTrade(models::EquityTrade {
            event_type: "T".into(),
            symbol: "X".into(),
            sequence_number: 0,
            ..Default::default()
        });
        assert!(tracker.check(&t).is_none());
    }

    #[test]
    fn test_sequence_tracker_clear() {
        let mut tracker = SequenceTracker::new();

        let t1 = MarketData::EquityTrade(models::EquityTrade {
            event_type: "T".into(),
            symbol: "SPY".into(),
            sequence_number: 100,
            ..Default::default()
        });
        tracker.check(&t1);
        tracker.clear();

        // After clear, seq=1 should not trigger a gap (treated as first-seen).
        let t2 = MarketData::EquityTrade(models::EquityTrade {
            event_type: "T".into(),
            symbol: "SPY".into(),
            sequence_number: 1,
            ..Default::default()
        });
        assert!(tracker.check(&t2).is_none());
    }

    #[test]
    fn test_sequence_tracker_ignores_non_sequenced_types() {
        let mut tracker = SequenceTracker::new();

        let agg = MarketData::EquityAgg(models::EquityAgg {
            event_type: "A".into(),
            symbol: "SPY".into(),
            ..Default::default()
        });
        assert!(tracker.check(&agg).is_none());
    }

    #[tokio::test]
    async fn test_gap_emitted_before_data() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("ws://127.0.0.1:{}", addr.port());

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            let (mut write, mut read) = ws.split();

            if let Some(Ok(_)) = read.next().await {
                let res = serde_json::json!([{"ev": "status", "status": "auth_success"}]);
                write
                    .send(Message::Text(res.to_string().into()))
                    .await
                    .unwrap();
            }
            if let Some(Ok(_)) = read.next().await {
                let res = serde_json::json!([{"ev": "status", "status": "success"}]);
                write
                    .send(Message::Text(res.to_string().into()))
                    .await
                    .unwrap();

                // seq 1, then skip to seq 5.
                let batch = serde_json::json!([
                    {"ev":"T","sym":"SPY","p":450.0,"s":100,"t":1_700_000_000_000_i64,"q":1},
                    {"ev":"T","sym":"SPY","p":451.0,"s":200,"t":1_700_000_000_001_i64,"q":5}
                ]);
                write
                    .send(Message::Text(batch.to_string().into()))
                    .await
                    .unwrap();
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        });

        let config = test_config("good", url);
        let (client, mut output, _errors) = Client::new(config).unwrap();

        client
            .subscribe(Topic::StocksTrades, &["SPY"])
            .await
            .unwrap();
        client.connect().await.unwrap();

        // First: the trade at seq=1 (no gap).
        let msg1 = tokio::time::timeout(Duration::from_secs(2), output.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(msg1, MarketData::EquityTrade(_)));

        // Second: the SequenceGap notification.
        let msg2 = tokio::time::timeout(Duration::from_secs(2), output.recv())
            .await
            .unwrap()
            .unwrap();
        match msg2 {
            MarketData::SequenceGap(gap) => {
                assert_eq!(gap.symbol, "SPY");
                assert_eq!(gap.event_type, "T");
                assert_eq!(gap.last_seen, 1);
                assert_eq!(gap.received, 5);
            }
            other => panic!("expected SequenceGap, got {other:?}"),
        }

        // Third: the trade at seq=5 (delivered after the gap event).
        let msg3 = tokio::time::timeout(Duration::from_secs(2), output.recv())
            .await
            .unwrap()
            .unwrap();
        match msg3 {
            MarketData::EquityTrade(t) => {
                assert_eq!(t.sequence_number, 5);
                assert_eq!(t.price, 451.0);
            }
            other => panic!("expected EquityTrade, got {other:?}"),
        }

        client.close().await;
    }
}
