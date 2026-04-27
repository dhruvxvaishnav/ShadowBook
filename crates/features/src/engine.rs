use crate::trades::TradeWindow;
use book::OrderBook;

pub const FEATURE_NAMES: &[&str] = &[
    "spread",
    "mid",
    "microprice",
    "microprice_diff",
    "imb_l1",
    "imb_l5",
    "imb_l10",
    "ofi_l1",
    "tfi",
    "trade_count",
    "realized_vol",
];

pub const FEATURE_LEN: usize = 11;

#[derive(Debug, Clone, Copy)]
pub struct FeatureVector(pub [f64; FEATURE_LEN]);

impl FeatureVector {
    pub fn as_slice(&self) -> &[f64] {
        &self.0
    }
}

pub struct FeatureEngine {
    trades: TradeWindow,
    prev_best_bid_qty: f64,
    prev_best_ask_qty: f64,
    prev_microprice: f64,
}

impl FeatureEngine {
    pub fn new(trade_window_ms: u64) -> Self {
        Self {
            trades: TradeWindow::new(trade_window_ms),
            prev_best_bid_qty: 0.0,
            prev_best_ask_qty: 0.0,
            prev_microprice: 0.0,
        }
    }

    pub fn trades_mut(&mut self) -> &mut TradeWindow {
        &mut self.trades
    }

    pub fn compute(&mut self, book: &OrderBook, now_ms: u64) -> FeatureVector {
        self.trades.evict_to(now_ms);
        let state = book.state();
        let mid = state.mid();
        let spread = state.spread();
        let microprice = state.microprice();

        let imb_l1 = imbalance_at_depth(book, 1);
        let imb_l5 = imbalance_at_depth(book, 5);
        let imb_l10 = imbalance_at_depth(book, 10);

        // Cont-Kukanov-Stoikov OFI at L1.
        let ofi_l1 = ofi_l1(
            state.best_bid_qty,
            self.prev_best_bid_qty,
            state.best_ask_qty,
            self.prev_best_ask_qty,
        );
        self.prev_best_bid_qty = state.best_bid_qty;
        self.prev_best_ask_qty = state.best_ask_qty;

        let microprice_diff = if self.prev_microprice == 0.0 {
            0.0
        } else {
            microprice - self.prev_microprice
        };
        self.prev_microprice = microprice;

        let total_vol = self.trades.total_volume();
        let tfi = if total_vol > 0.0 {
            self.trades.signed_volume() / total_vol
        } else {
            0.0
        };

        let mut v = [0.0; FEATURE_LEN];
        v[0] = spread;
        v[1] = mid;
        v[2] = microprice;
        v[3] = microprice_diff;
        v[4] = imb_l1;
        v[5] = imb_l5;
        v[6] = imb_l10;
        v[7] = ofi_l1;
        v[8] = tfi;
        v[9] = self.trades.len() as f64;
        v[10] = self.trades.realized_vol();
        FeatureVector(v)
    }
}

fn imbalance_at_depth(book: &OrderBook, depth: usize) -> f64 {
    let bid_sum: f64 = book.bid_levels().rev().take(depth).map(|(_, q)| q).sum();
    let ask_sum: f64 = book.ask_levels().take(depth).map(|(_, q)| q).sum();
    let denom = bid_sum + ask_sum;
    if denom == 0.0 {
        0.0
    } else {
        (bid_sum - ask_sum) / denom
    }
}

fn ofi_l1(bid_qty: f64, prev_bid_qty: f64, ask_qty: f64, prev_ask_qty: f64) -> f64 {
    (bid_qty - prev_bid_qty) - (ask_qty - prev_ask_qty)
}

#[cfg(test)]
mod tests {
    use super::*;
    use feed::messages::{PriceLevel, SnapshotResponse};
    use ordered_float::OrderedFloat;

    fn pl(price: f64, qty: f64) -> PriceLevel {
        PriceLevel {
            price: OrderedFloat(price),
            qty: OrderedFloat(qty),
        }
    }

    fn book_with(bids: Vec<(f64, f64)>, asks: Vec<(f64, f64)>) -> OrderBook {
        let mut b = OrderBook::new();
        b.apply_snapshot(SnapshotResponse {
            last_update_id: 1,
            bids: bids.into_iter().map(|(p, q)| pl(p, q)).collect(),
            asks: asks.into_iter().map(|(p, q)| pl(p, q)).collect(),
        })
        .unwrap();
        b
    }

    #[test]
    fn imbalance_balanced_book_zero() {
        let b = book_with(vec![(100.0, 1.0)], vec![(101.0, 1.0)]);
        assert!(imbalance_at_depth(&b, 1).abs() < 1e-9);
    }

    #[test]
    fn imbalance_bid_heavy_positive() {
        let b = book_with(vec![(100.0, 3.0)], vec![(101.0, 1.0)]);
        let i = imbalance_at_depth(&b, 1);
        assert!((i - 0.5).abs() < 1e-9);
    }

    #[test]
    fn imbalance_at_depth_5_sums_top_levels() {
        let b = book_with(
            vec![(100.0, 1.0), (99.0, 1.0), (98.0, 1.0)],
            vec![(101.0, 2.0), (102.0, 2.0)],
        );
        // bid sum 3, ask sum 4, imb = -1/7
        let i = imbalance_at_depth(&b, 5);
        assert!((i - (-1.0 / 7.0)).abs() < 1e-9);
    }

    #[test]
    fn ofi_l1_basic() {
        assert!((ofi_l1(3.0, 2.0, 1.0, 2.0) - 2.0).abs() < 1e-9);
    }

    #[test]
    fn engine_produces_full_vector() {
        let b = book_with(vec![(100.0, 1.0)], vec![(101.0, 1.0)]);
        let mut e = FeatureEngine::new(1000);
        let v = e.compute(&b, 0);
        assert_eq!(v.as_slice().len(), FEATURE_LEN);
        assert_eq!(FEATURE_NAMES.len(), FEATURE_LEN);
    }
}
