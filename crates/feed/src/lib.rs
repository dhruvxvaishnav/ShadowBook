//! Binance WebSocket feed client and message types.
//!
//! Subscribes to depth diff and trade streams, parses messages, and emits
//! typed events downstream. Also provides a REST client for fetching the
//! initial order book snapshot.

#![deny(unsafe_code)]

pub mod error;
pub mod messages;
pub mod rest;
pub mod ws;

pub use error::{FeedError, Result};
pub use messages::{DepthUpdate, MarketEvent, PriceLevel, SnapshotResponse, Trade};
pub use rest::fetch_snapshot;
pub use ws::run_feed;
