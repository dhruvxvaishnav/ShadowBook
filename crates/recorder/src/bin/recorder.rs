//! Weekend 1 entry point: connect to Binance, reconstruct the book,
//! write raw events + periodic book snapshots to Parquet.

use anyhow::{Context, Result};
use book::OrderBook;
use chrono::Utc;
use feed::{fetch_snapshot, run_feed, MarketEvent};
use recorder::{EventWriter, SnapshotWriter};
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

const SYMBOL: &str = "BTCUSDT";
const CHANNEL_CAP: usize = 8_192;
const SNAPSHOT_DEPTH: u32 = 1000;
const STATE_WRITE_INTERVAL_MS: u64 = 100;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let ts = Utc::now().format("%Y%m%d_%H%M%S");
    let data_dir = PathBuf::from("data");
    let events_path = data_dir.join(format!("events_{ts}.parquet"));
    let snaps_path = data_dir.join(format!("book_{ts}.parquet"));

    tracing::info!(?events_path, ?snaps_path, "starting recorder");

    let (tx, mut rx) = mpsc::channel::<MarketEvent>(CHANNEL_CAP);

    let feed_handle = tokio::spawn(async move {
        if let Err(e) = run_feed(SYMBOL, tx).await {
            tracing::error!(error = %e, "feed exited");
        }
    });

    tokio::time::sleep(Duration::from_millis(20000)).await;

    let snapshot = fetch_snapshot(SYMBOL, SNAPSHOT_DEPTH)
        .await
        .context("initial snapshot")?;

    let mut book = OrderBook::new();
    let mut event_writer = EventWriter::create(&events_path)?;
    let mut snap_writer = SnapshotWriter::create(&snaps_path)?;

    let mut drained = 0usize;
    while let Ok(ev) = rx.try_recv() {
        if let MarketEvent::Depth(d) = &ev {
            book.buffer_diff(d.clone());
            drained += 1;
        }
        event_writer.write_event(&ev, now_ns())?;
    }
    tracing::info!(drained, "pre-snapshot diffs buffered");

    if let Err(e) = book.apply_snapshot(snapshot) {
        tracing::error!(error = %e, "initial snapshot application failed; re-sync required");
        anyhow::bail!("snapshot sync failed: {e}");
    }

    let mut last_state_write = SystemTime::now();
    let mut desync_count = 0u64;

    let mut shutdown = Box::pin(tokio::signal::ctrl_c());
    loop {
        let ev = tokio::select! {
            maybe = rx.recv() => match maybe {
                Some(ev) => ev,
                None => break,
            },
            _ = &mut shutdown => {
                tracing::info!("ctrl-c received, shutting down");
                break;
            }
        };

        event_writer.write_event(&ev, now_ns())?;

        if let MarketEvent::Depth(d) = &ev {
            match book.apply_diff(d) {
                Ok(()) => {}
                Err(book::BookError::StaleUpdate { .. }) => {}
                Err(e) => {
                    desync_count += 1;
                    tracing::error!(error = %e, desync_count, "book desync");
                    if desync_count > 5 {
                        anyhow::bail!("too many desyncs, exiting");
                    }
                }
            }
        }

        if last_state_write.elapsed().unwrap_or_default()
            >= Duration::from_millis(STATE_WRITE_INTERVAL_MS)
            && book.is_initialized()
        {
            snap_writer.write_state(&book.state(), now_ns())?;
            last_state_write = SystemTime::now();
        }
    }

    tracing::info!("closing writers");
    event_writer.close()?;
    snap_writer.close()?;
    feed_handle.abort();
    Ok(())
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}
