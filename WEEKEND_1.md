# Weekend 1 — Quickstart

## Prerequisites

Install Rust (stable) if you haven't:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Verify:

```bash
rustc --version   # should be 1.75+
cargo --version
```

## First run

From the repo root:

```bash
# 1. Build everything (downloads deps, takes a few minutes first time)
cargo build --workspace

# 2. Run the unit tests — book + feed parsing must pass
cargo test --workspace

# 3. Check lints
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings

# 4. Run the recorder. It connects to Binance and writes to ./data/
RUST_LOG=info cargo run --release --bin recorder
```

You should see log lines like:

```
INFO starting recorder events_path=... snaps_path=...
INFO connecting to binance ws url=...
INFO websocket connected
INFO fetching snapshot symbol="BTCUSDT" limit=1000
INFO snapshot received last_update_id=... bids=1000 asks=1000
INFO pre-snapshot diffs buffered drained=3
INFO snapshot applied last_update_id=... bids=1000 asks=1000
INFO replayed buffered diffs applied=2 skipped=1
INFO feed stats depth_count=1000 trade_count=... drop_count=0
```

Hit `Ctrl+C` to stop. Parquet files land in `./data/`.

## What to validate this weekend

### 1. Unit tests pass
```bash
cargo test --workspace
```

### 2. Live run is stable
Let it run for 1 hour. Look for:
- `desync_count` stays at 0
- `drop_count` stays at 0 (or very low)
- `feed stats` line increments steadily every ~1000 messages
- No `ERROR` lines

### 3. Parity check
Open https://www.binance.com/en/trade/BTC_USDT in a browser. Compare your logged best bid/ask to the site's top of book. They should match within one tick at any given second.

A quick script you can drop in `python/sanity.py`:

```python
import pyarrow.parquet as pq
import pandas as pd

t = pq.read_table("data/book_YYYYMMDD_HHMMSS.parquet").to_pandas()
print(t.tail())
print("rows:", len(t))
print("update_id monotonic?", t["last_update_id"].is_monotonic_increasing)
print("spread stats:")
print((t["best_ask"] - t["best_bid"]).describe())
```

## Troubleshooting

**Build error: `failed to fetch ...`**
You're probably behind a firewall or the crates.io mirror is slow. Retry `cargo build`.

**`SequenceGap` error at startup**
The pre-snapshot buffer didn't cover the gap. Rerun — the timing is slightly probabilistic. If it happens repeatedly, increase the `sleep(500ms)` before the snapshot fetch in `recorder.rs` to 1000ms.

**High `drop_count`**
The downstream pipeline is slower than the feed. Weekend 1: raise `CHANNEL_CAP`. Longer term: profile.

**`Crossed book` error**
Almost always a bug in diff application, not a real exchange event. Open an issue and attach the last ~100 log lines.

## Weekend 1 exit criteria

- [ ] `cargo test --workspace` passes
- [ ] `cargo clippy --workspace -- -D warnings` clean
- [ ] Recorder runs for 1h with `desync_count == 0` and `drop_count == 0`
- [ ] Spot-check: best bid/ask in `book_*.parquet` matches Binance UI
- [ ] `events_*.parquet` and `book_*.parquet` both readable by pandas/pyarrow

When all four are checked, Weekend 1 is done. Weekend 2 is feature engineering and LightGBM training on the Parquet you just recorded.
