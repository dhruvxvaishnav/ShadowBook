//! REST client for fetching the initial order book snapshot.

use crate::error::Result;
use crate::messages::SnapshotResponse;

const BINANCE_REST_BASE: &str = "https://api.binance.com";

/// Fetch a depth snapshot for a symbol.
///
/// `limit` must be one of 5, 10, 20, 50, 100, 500, 1000, 5000.
/// For order book reconstruction we recommend 1000 or 5000.
pub async fn fetch_snapshot(symbol: &str, limit: u32) -> Result<SnapshotResponse> {
    let url = format!(
        "{}/api/v3/depth?symbol={}&limit={}",
        BINANCE_REST_BASE,
        symbol.to_uppercase(),
        limit
    );
    tracing::info!(symbol, limit, "fetching snapshot");
    let resp = reqwest::get(&url).await?.error_for_status()?;
    let snap: SnapshotResponse = resp.json().await?;
    tracing::info!(
        last_update_id = snap.last_update_id,
        bids = snap.bids.len(),
        asks = snap.asks.len(),
        "snapshot received"
    );
    Ok(snap)
}
