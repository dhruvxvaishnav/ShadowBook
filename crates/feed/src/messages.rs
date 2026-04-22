//! Typed message definitions for Binance spot market data.
//!
//! Reference: <https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams>

use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

/// A single price level: (price, quantity).
///
/// Binance sends these as `["1234.56", "0.01"]` string tuples.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PriceLevel {
    pub price: OrderedFloat<f64>,
    pub qty: OrderedFloat<f64>,
}

impl<'de> Deserialize<'de> for PriceLevel {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let tuple: [String; 2] = Deserialize::deserialize(deserializer)?;
        let price = tuple[0].parse::<f64>().map_err(serde::de::Error::custom)?;
        let qty = tuple[1].parse::<f64>().map_err(serde::de::Error::custom)?;
        Ok(PriceLevel {
            price: OrderedFloat(price),
            qty: OrderedFloat(qty),
        })
    }
}

impl Serialize for PriceLevel {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeTuple;
        let mut t = serializer.serialize_tuple(2)?;
        t.serialize_element(&self.price.0.to_string())?;
        t.serialize_element(&self.qty.0.to_string())?;
        t.end()
    }
}

/// Depth diff update: `<symbol>@depth@100ms`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DepthUpdate {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E")]
    pub event_time_ms: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    /// First update ID in event.
    #[serde(rename = "U")]
    pub first_update_id: u64,
    /// Final update ID in event.
    #[serde(rename = "u")]
    pub final_update_id: u64,
    #[serde(rename = "b")]
    pub bids: Vec<PriceLevel>,
    #[serde(rename = "a")]
    pub asks: Vec<PriceLevel>,
}

/// Trade event: `<symbol>@trade`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Trade {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E")]
    pub event_time_ms: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "t")]
    pub trade_id: u64,
    #[serde(rename = "p")]
    pub price: String,
    #[serde(rename = "q")]
    pub qty: String,
    #[serde(rename = "T")]
    pub trade_time_ms: u64,
    /// `true` means the buyer is the market maker (i.e. sell-side aggressor).
    #[serde(rename = "m")]
    pub buyer_is_maker: bool,
}

impl Trade {
    pub fn price_f64(&self) -> f64 {
        self.price.parse().unwrap_or(0.0)
    }

    pub fn qty_f64(&self) -> f64 {
        self.qty.parse().unwrap_or(0.0)
    }
}

/// REST snapshot response from `/api/v3/depth`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SnapshotResponse {
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: u64,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
}

/// Unified event emitted by the feed task to downstream consumers.
#[derive(Debug, Clone)]
pub enum MarketEvent {
    Depth(DepthUpdate),
    Trade(Trade),
}

impl MarketEvent {
    pub fn event_time_ms(&self) -> u64 {
        match self {
            MarketEvent::Depth(d) => d.event_time_ms,
            MarketEvent::Trade(t) => t.event_time_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_depth_update() {
        let raw = r#"{
            "e":"depthUpdate","E":1700000000000,"s":"BTCUSDT",
            "U":100,"u":105,
            "b":[["50000.00","1.0"],["49999.00","0.5"]],
            "a":[["50001.00","0.8"]]
        }"#;
        let d: DepthUpdate = serde_json::from_str(raw).unwrap();
        assert_eq!(d.symbol, "BTCUSDT");
        assert_eq!(d.first_update_id, 100);
        assert_eq!(d.final_update_id, 105);
        assert_eq!(d.bids.len(), 2);
        assert_eq!(d.bids[0].price, OrderedFloat(50000.00));
        assert_eq!(d.asks[0].qty, OrderedFloat(0.8));
    }

    #[test]
    fn parse_trade() {
        let raw = r#"{
            "e":"trade","E":1700000000000,"s":"BTCUSDT",
            "t":12345,"p":"50000.50","q":"0.01",
            "T":1700000000000,"m":false
        }"#;
        let t: Trade = serde_json::from_str(raw).unwrap();
        assert_eq!(t.trade_id, 12345);
        assert!((t.price_f64() - 50000.50).abs() < 1e-9);
        assert!(!t.buyer_is_maker);
    }

    #[test]
    fn parse_snapshot() {
        let raw = r#"{
            "lastUpdateId":1027024,
            "bids":[["4.00000000","431.00000000"]],
            "asks":[["4.00000200","12.00000000"]]
        }"#;
        let s: SnapshotResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(s.last_update_id, 1027024);
        assert_eq!(s.bids.len(), 1);
    }
}
