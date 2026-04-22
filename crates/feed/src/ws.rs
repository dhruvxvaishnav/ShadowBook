//! Binance combined-stream WebSocket client.
//!
//! Subscribes to `<symbol>@depth@100ms` and `<symbol>@trade`, parses messages,
//! and forwards `MarketEvent`s through a bounded channel.

use crate::error::{FeedError, Result};
use crate::messages::{DepthUpdate, MarketEvent, Trade};
use futures_util::StreamExt;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

const BINANCE_WS_BASE: &str = "wss://stream.binance.com:9443/stream?streams=";

/// Envelope for Binance combined-stream messages: `{"stream":..., "data":...}`.
#[derive(Debug, Deserialize)]
struct CombinedStreamMessage {
    stream: String,
    data: serde_json::Value,
}

/// Connect to Binance combined stream for `symbol` and forward parsed events.
///
/// The `tx` channel should be bounded; on overflow we drop messages and log.
pub async fn run_feed(symbol: &str, tx: mpsc::Sender<MarketEvent>) -> Result<()> {
    let sym = symbol.to_lowercase();
    let url = format!("{}{sym}@depth@100ms/{sym}@trade", BINANCE_WS_BASE);
    tracing::info!(%url, "connecting to binance ws");

    let (ws_stream, _) = connect_async(&url).await?;
    let (_write, mut read) = ws_stream.split();
    tracing::info!("websocket connected");

    let mut depth_count: u64 = 0;
    let mut trade_count: u64 = 0;
    let mut drop_count: u64 = 0;

    while let Some(msg) = read.next().await {
        let msg = msg?;
        match msg {
            Message::Text(txt) => {
                let event = match parse_combined(&txt) {
                    Ok(Some(ev)) => ev,
                    Ok(None) => continue,
                    Err(e) => {
                        tracing::warn!(error = %e, "parse error");
                        continue;
                    }
                };
                match &event {
                    MarketEvent::Depth(_) => depth_count += 1,
                    MarketEvent::Trade(_) => trade_count += 1,
                }
                // Drop-oldest policy: if receiver is full, skip rather than block.
                if tx.try_send(event).is_err() {
                    drop_count += 1;
                    if drop_count.is_power_of_two() {
                        tracing::warn!(drop_count, "downstream full, dropping events");
                    }
                }
                if (depth_count + trade_count) % 1000 == 0 {
                    tracing::info!(depth_count, trade_count, drop_count, "feed stats");
                }
            }
            Message::Ping(p) => tracing::trace!(len = p.len(), "ping"),
            Message::Pong(_) => {}
            Message::Close(c) => {
                tracing::warn!(?c, "server closed connection");
                break;
            }
            _ => {}
        }
    }
    Err(FeedError::ChannelClosed)
}

fn parse_combined(txt: &str) -> Result<Option<MarketEvent>> {
    let env: CombinedStreamMessage = serde_json::from_str(txt)?;
    if env.stream.ends_with("@trade") {
        let t: Trade = serde_json::from_value(env.data)?;
        Ok(Some(MarketEvent::Trade(t)))
    } else if env.stream.contains("@depth") {
        let d: DepthUpdate = serde_json::from_value(env.data)?;
        Ok(Some(MarketEvent::Depth(d)))
    } else {
        Ok(None)
    }
}
