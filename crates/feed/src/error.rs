//! Error types for the feed crate.

use thiserror::Error;

pub type Result<T> = std::result::Result<T, FeedError>;

#[derive(Debug, Error)]
pub enum FeedError {
    #[error("websocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("json parse error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("sequence gap: expected {expected}, got {got}")]
    SequenceGap { expected: u64, got: u64 },

    #[error("malformed message: {0}")]
    Malformed(String),

    #[error("channel closed")]
    ChannelClosed,

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
