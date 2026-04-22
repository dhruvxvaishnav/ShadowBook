//! Error types for order book reconstruction.

use thiserror::Error;

pub type Result<T> = std::result::Result<T, BookError>;

#[derive(Debug, Error)]
pub enum BookError {
    #[error("book not initialized; snapshot required first")]
    NotInitialized,

    #[error("sequence gap: last_update_id={last}, incoming first_update_id={first}")]
    SequenceGap { last: u64, first: u64 },

    #[error("crossed book: best_bid={bid} >= best_ask={ask}")]
    Crossed { bid: f64, ask: f64 },

    #[error("stale update: final_update_id={final_id} <= last_update_id={last}")]
    StaleUpdate { final_id: u64, last: u64 },
}
