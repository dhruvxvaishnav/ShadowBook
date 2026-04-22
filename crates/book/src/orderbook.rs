//! Order book data structure and update logic.

use crate::error::{BookError, Result};
use feed::messages::{DepthUpdate, PriceLevel, SnapshotResponse};
use ordered_float::OrderedFloat;
use std::collections::BTreeMap;

/// Public-facing summary of the book at an instant, cheap to clone.
#[derive(Debug, Clone, Copy, Default)]
pub struct BookState {
    pub best_bid: f64,
    pub best_bid_qty: f64,
    pub best_ask: f64,
    pub best_ask_qty: f64,
    pub last_update_id: u64,
    pub event_time_ms: u64,
}

impl BookState {
    pub fn mid(&self) -> f64 {
        (self.best_bid + self.best_ask) / 2.0
    }

    pub fn spread(&self) -> f64 {
        self.best_ask - self.best_bid
    }

    /// Stoikov microprice: size-weighted fair value between best bid and ask.
    pub fn microprice(&self) -> f64 {
        let denom = self.best_bid_qty + self.best_ask_qty;
        if denom == 0.0 {
            return self.mid();
        }
        (self.best_bid * self.best_ask_qty + self.best_ask * self.best_bid_qty) / denom
    }
}

/// Local L2 order book.
///
/// Bids are stored in a `BTreeMap` keyed by `OrderedFloat<f64>`; the map is
/// sorted ascending, so the best bid is `.iter().next_back()`. Asks are
/// likewise ascending with best ask at `.iter().next()`.
#[derive(Debug, Default)]
pub struct OrderBook {
    bids: BTreeMap<OrderedFloat<f64>, f64>,
    asks: BTreeMap<OrderedFloat<f64>, f64>,
    last_update_id: u64,
    initialized: bool,
    /// Buffer of diffs received before the snapshot arrived.
    pending: Vec<DepthUpdate>,
    last_event_time_ms: u64,
}

impl OrderBook {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_initialized(&self) -> bool {
        self.initialized
    }

    pub fn last_update_id(&self) -> u64 {
        self.last_update_id
    }

    pub fn bid_levels(&self) -> impl DoubleEndedIterator<Item = (f64, f64)> + '_ {
        self.bids.iter().map(|(p, q)| (p.0, *q))
    }

    pub fn ask_levels(&self) -> impl Iterator<Item = (f64, f64)> + '_ {
        self.asks.iter().map(|(p, q)| (p.0, *q))
    }

    pub fn best_bid(&self) -> Option<(f64, f64)> {
        self.bids.iter().next_back().map(|(p, q)| (p.0, *q))
    }

    pub fn best_ask(&self) -> Option<(f64, f64)> {
        self.asks.iter().next().map(|(p, q)| (p.0, *q))
    }

    pub fn state(&self) -> BookState {
        let (bb, bbq) = self.best_bid().unwrap_or((0.0, 0.0));
        let (ba, baq) = self.best_ask().unwrap_or((0.0, 0.0));
        BookState {
            best_bid: bb,
            best_bid_qty: bbq,
            best_ask: ba,
            best_ask_qty: baq,
            last_update_id: self.last_update_id,
            event_time_ms: self.last_event_time_ms,
        }
    }

    /// Buffer a diff received before the snapshot. Applied during `apply_snapshot`.
    pub fn buffer_diff(&mut self, d: DepthUpdate) {
        self.pending.push(d);
    }

    /// Apply the REST snapshot and replay any buffered diffs per Binance spec:
    ///
    /// 1. Buffer diffs as they arrive.
    /// 2. Fetch snapshot with `lastUpdateId = L`.
    /// 3. Drop buffered diffs where `final_update_id <= L`.
    /// 4. The first kept diff must satisfy `first_update_id <= L+1 <= final_update_id`.
    /// 5. Apply that diff and all subsequent ones normally.
    pub fn apply_snapshot(&mut self, snap: SnapshotResponse) -> Result<()> {
        self.bids.clear();
        self.asks.clear();
        for lvl in snap.bids {
            insert_level(&mut self.bids, lvl);
        }
        for lvl in snap.asks {
            insert_level(&mut self.asks, lvl);
        }
        self.last_update_id = snap.last_update_id;
        self.initialized = true;
        tracing::info!(
            last_update_id = self.last_update_id,
            bids = self.bids.len(),
            asks = self.asks.len(),
            "snapshot applied"
        );

        let pending = std::mem::take(&mut self.pending);
        let mut applied = 0usize;
        let mut skipped = 0usize;
        let mut found_anchor = false;
        for d in pending {
            if d.final_update_id <= self.last_update_id {
                skipped += 1;
                continue;
            }
            if !found_anchor {
                if d.first_update_id <= self.last_update_id + 1
                    && d.final_update_id >= self.last_update_id + 1
                {
                    // Anchor diff straddles the snapshot boundary. Apply
                    // its levels directly, bypassing strict continuity.
                    for lvl in &d.bids {
                        insert_level(&mut self.bids, *lvl);
                    }
                    for lvl in &d.asks {
                        insert_level(&mut self.asks, *lvl);
                    }
                    self.last_update_id = d.final_update_id;
                    self.last_event_time_ms = d.event_time_ms;
                    found_anchor = true;
                    applied += 1;
                    continue;
                } else {
                    return Err(BookError::SequenceGap {
                        last: self.last_update_id,
                        first: d.first_update_id,
                    });
                }
            }
            self.apply_diff(&d)?;
            applied += 1;
        }
        tracing::info!(applied, skipped, "replayed buffered diffs");
        Ok(())
    }

    /// Apply a depth diff to the book. Assumes the sequence anchor has already been
    /// established (either by `apply_snapshot` or previous `apply_diff` calls).
    pub fn apply_diff(&mut self, d: &DepthUpdate) -> Result<()> {
        if !self.initialized {
            return Err(BookError::NotInitialized);
        }
        if d.final_update_id <= self.last_update_id {
            return Err(BookError::StaleUpdate {
                final_id: d.final_update_id,
                last: self.last_update_id,
            });
        }
        // Strict continuity: first_update_id must equal last_update_id + 1.
        // Note: the first post-snapshot diff is allowed to span the boundary,
        // handled by `apply_snapshot`.
        if d.first_update_id != self.last_update_id + 1 {
            return Err(BookError::SequenceGap {
                last: self.last_update_id,
                first: d.first_update_id,
            });
        }
        for lvl in &d.bids {
            insert_level(&mut self.bids, *lvl);
        }
        for lvl in &d.asks {
            insert_level(&mut self.asks, *lvl);
        }
        self.last_update_id = d.final_update_id;
        self.last_event_time_ms = d.event_time_ms;
        self.check_not_crossed()?;
        Ok(())
    }

    fn check_not_crossed(&self) -> Result<()> {
        if let (Some((bb, _)), Some((ba, _))) = (self.best_bid(), self.best_ask()) {
            if bb >= ba {
                return Err(BookError::Crossed { bid: bb, ask: ba });
            }
        }
        Ok(())
    }
}

fn insert_level(map: &mut BTreeMap<OrderedFloat<f64>, f64>, lvl: PriceLevel) {
    if lvl.qty.0 == 0.0 {
        map.remove(&lvl.price);
    } else {
        map.insert(lvl.price, lvl.qty.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pl(price: f64, qty: f64) -> PriceLevel {
        PriceLevel {
            price: OrderedFloat(price),
            qty: OrderedFloat(qty),
        }
    }

    fn snap(last: u64, bids: Vec<(f64, f64)>, asks: Vec<(f64, f64)>) -> SnapshotResponse {
        SnapshotResponse {
            last_update_id: last,
            bids: bids.into_iter().map(|(p, q)| pl(p, q)).collect(),
            asks: asks.into_iter().map(|(p, q)| pl(p, q)).collect(),
        }
    }

    fn diff(first: u64, final_: u64, bids: Vec<(f64, f64)>, asks: Vec<(f64, f64)>) -> DepthUpdate {
        DepthUpdate {
            event_type: "depthUpdate".into(),
            event_time_ms: 0,
            symbol: "BTCUSDT".into(),
            first_update_id: first,
            final_update_id: final_,
            bids: bids.into_iter().map(|(p, q)| pl(p, q)).collect(),
            asks: asks.into_iter().map(|(p, q)| pl(p, q)).collect(),
        }
    }

    #[test]
    fn empty_book_has_no_best() {
        let b = OrderBook::new();
        assert!(b.best_bid().is_none());
        assert!(b.best_ask().is_none());
    }

    #[test]
    fn snapshot_applies_and_best_levels_correct() {
        let mut b = OrderBook::new();
        b.apply_snapshot(snap(
            10,
            vec![(100.0, 1.0), (99.0, 2.0)],
            vec![(101.0, 1.5)],
        ))
        .unwrap();
        assert_eq!(b.best_bid(), Some((100.0, 1.0)));
        assert_eq!(b.best_ask(), Some((101.0, 1.5)));
        assert_eq!(b.last_update_id(), 10);
    }

    #[test]
    fn zero_qty_removes_level() {
        let mut b = OrderBook::new();
        b.apply_snapshot(snap(
            10,
            vec![(100.0, 1.0), (99.0, 2.0)],
            vec![(101.0, 1.5)],
        ))
        .unwrap();
        b.apply_diff(&diff(11, 11, vec![(99.0, 0.0)], vec![]))
            .unwrap();
        assert_eq!(b.best_bid(), Some((100.0, 1.0)));
        assert_eq!(b.bid_levels().count(), 1);
    }

    #[test]
    fn sequence_gap_rejected() {
        let mut b = OrderBook::new();
        b.apply_snapshot(snap(10, vec![(100.0, 1.0)], vec![(101.0, 1.0)]))
            .unwrap();
        let res = b.apply_diff(&diff(15, 16, vec![(100.5, 1.0)], vec![]));
        assert!(matches!(res, Err(BookError::SequenceGap { .. })));
    }

    #[test]
    fn stale_update_rejected() {
        let mut b = OrderBook::new();
        b.apply_snapshot(snap(10, vec![(100.0, 1.0)], vec![(101.0, 1.0)]))
            .unwrap();
        let res = b.apply_diff(&diff(5, 9, vec![], vec![]));
        assert!(matches!(res, Err(BookError::StaleUpdate { .. })));
    }

    #[test]
    fn crossed_book_rejected() {
        let mut b = OrderBook::new();
        b.apply_snapshot(snap(10, vec![(100.0, 1.0)], vec![(101.0, 1.0)]))
            .unwrap();
        // Push bid up above ask.
        let res = b.apply_diff(&diff(11, 11, vec![(102.0, 1.0)], vec![]));
        assert!(matches!(res, Err(BookError::Crossed { .. })));
    }

    #[test]
    fn buffered_diffs_replayed_after_snapshot() {
        let mut b = OrderBook::new();
        // Buffer two diffs received before snapshot.
        b.buffer_diff(diff(5, 8, vec![(99.0, 5.0)], vec![]));
        b.buffer_diff(diff(9, 12, vec![(98.0, 3.0)], vec![(101.5, 2.0)]));
        // Snapshot at last_update_id = 10; first diff is stale, second spans the anchor.
        b.apply_snapshot(snap(10, vec![(100.0, 1.0)], vec![(101.0, 1.0)]))
            .unwrap();
        assert_eq!(b.last_update_id(), 12);
        assert_eq!(b.best_bid(), Some((100.0, 1.0)));
        // 98.0 level should exist from replayed diff.
        assert!(b.bid_levels().any(|(p, _)| p == 98.0));
        assert!(b.ask_levels().any(|(p, _)| p == 101.5));
    }

    #[test]
    fn microprice_between_bid_and_ask() {
        let mut b = OrderBook::new();
        b.apply_snapshot(snap(10, vec![(100.0, 1.0)], vec![(101.0, 1.0)]))
            .unwrap();
        let s = b.state();
        let mp = s.microprice();
        assert!(mp >= s.best_bid && mp <= s.best_ask);
    }
}
