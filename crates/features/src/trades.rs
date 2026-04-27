//! Rolling window of recent trades for trade-flow features.

use feed::messages::Trade;
use std::collections::VecDeque;

/// Fixed-duration rolling trade window.
#[derive(Debug)]
pub struct TradeWindow {
    window_ms: u64,
    trades: VecDeque<Trade>,
}

impl TradeWindow {
    pub fn new(window_ms: u64) -> Self {
        Self {
            window_ms,
            trades: VecDeque::with_capacity(1024),
        }
    }

    /// Push a trade and evict any older than `window_ms` from `now_ms`.
    pub fn push(&mut self, t: Trade, now_ms: u64) {
        self.trades.push_back(t);
        let cutoff = now_ms.saturating_sub(self.window_ms);
        while let Some(front) = self.trades.front() {
            if front.trade_time_ms < cutoff {
                self.trades.pop_front();
            } else {
                break;
            }
        }
    }

    pub fn evict_to(&mut self, now_ms: u64) {
        let cutoff = now_ms.saturating_sub(self.window_ms);
        while let Some(front) = self.trades.front() {
            if front.trade_time_ms < cutoff {
                self.trades.pop_front();
            } else {
                break;
            }
        }
    }

    pub fn len(&self) -> usize {
        self.trades.len()
    }

    pub fn is_empty(&self) -> bool {
        self.trades.is_empty()
    }

    /// Buy volume minus sell volume in the window.
    /// Buy aggressor = `!buyer_is_maker` per Binance convention.
    pub fn signed_volume(&self) -> f64 {
        let mut sum = 0.0;
        for t in &self.trades {
            let q = t.qty_f64();
            if t.buyer_is_maker {
                sum -= q;
            } else {
                sum += q;
            }
        }
        sum
    }

    pub fn total_volume(&self) -> f64 {
        self.trades.iter().map(|t| t.qty_f64()).sum()
    }

    /// Realized volatility over the window (std of log returns between consecutive trades).
    pub fn realized_vol(&self) -> f64 {
        if self.trades.len() < 2 {
            return 0.0;
        }
        let mut prev = self.trades[0].price_f64();
        let mut returns = Vec::with_capacity(self.trades.len());
        for t in self.trades.iter().skip(1) {
            let p = t.price_f64();
            if prev > 0.0 && p > 0.0 {
                returns.push((p / prev).ln());
            }
            prev = p;
        }
        if returns.is_empty() {
            return 0.0;
        }
        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let var = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / returns.len() as f64;
        var.sqrt()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trade(id: u64, price: f64, qty: f64, time_ms: u64, buyer_is_maker: bool) -> Trade {
        Trade {
            event_type: "trade".into(),
            event_time_ms: time_ms,
            symbol: "BTCUSDT".into(),
            trade_id: id,
            price: price.to_string(),
            qty: qty.to_string(),
            trade_time_ms: time_ms,
            buyer_is_maker,
        }
    }

    #[test]
    fn empty_window_zero_signals() {
        let w = TradeWindow::new(1000);
        assert_eq!(w.signed_volume(), 0.0);
        assert_eq!(w.total_volume(), 0.0);
        assert_eq!(w.realized_vol(), 0.0);
    }

    #[test]
    fn signed_volume_buys_minus_sells() {
        let mut w = TradeWindow::new(1000);
        // Buy aggressor: buyer_is_maker = false
        w.push(trade(1, 100.0, 2.0, 100, false), 100);
        // Sell aggressor: buyer_is_maker = true
        w.push(trade(2, 100.0, 1.0, 200, true), 200);
        assert!((w.signed_volume() - 1.0).abs() < 1e-9);
        assert!((w.total_volume() - 3.0).abs() < 1e-9);
    }

    #[test]
    fn old_trades_evicted() {
        let mut w = TradeWindow::new(1000);
        w.push(trade(1, 100.0, 1.0, 100, false), 100);
        w.push(trade(2, 100.0, 1.0, 500, false), 500);
        w.push(trade(3, 100.0, 1.0, 1500, false), 1500);
        // Window cutoff = 500, so trade at 100 is evicted.
        assert_eq!(w.len(), 2);
    }
}
