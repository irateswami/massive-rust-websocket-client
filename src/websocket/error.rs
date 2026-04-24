use crate::websocket::config::{Market, Topic};

/// Errors that can occur in the WebSocket client.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("authentication failed: {0}")]
    AuthFailed(String),

    #[error("connection failed: {0}")]
    ConnectionFailed(#[from] tungstenite::Error),

    #[error("max reconnection attempts exceeded")]
    MaxRetriesExceeded,

    #[error("max connections exceeded: {0}")]
    MaxConnections(String),

    #[error("invalid config: {0}")]
    InvalidConfig(String),

    #[error("unsupported topic {0:?} for market {1:?}")]
    UnsupportedTopic(Topic, Market),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("channel closed")]
    ChannelClosed,
}
