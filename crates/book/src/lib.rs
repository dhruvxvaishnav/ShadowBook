//! L2 order book reconstruction from Binance depth diffs.
//!
//! Implements the synchronization protocol:
//! <https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#how-to-manage-a-local-order-book-correctly>

#![deny(unsafe_code)]

pub mod error;
pub mod orderbook;

pub use error::{BookError, Result};
pub use orderbook::{BookState, OrderBook};
