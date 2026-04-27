//! Offline replay: events_*.parquet -> features_*.parquet
//!
//! Reads recorded depth + trade events in order, drives the order book and
//! feature engine, labels each row with the sign of mid-price return over a
//! configurable horizon, and writes a flat features Parquet for training.

use anyhow::{Context, Result};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Builder, Int8Builder, ListArray, StringArray,
    StructArray, UInt64Array, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use book::OrderBook;
use feed::messages::{DepthUpdate, PriceLevel, Trade};
use features::{FeatureEngine, FEATURE_NAMES};
use ordered_float::OrderedFloat;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::collections::VecDeque;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const LABEL_HORIZON_MS: u64 = 10_000;
const TRADE_WINDOW_MS: u64 = 5_000;
const FLUSH_EVERY: usize = 5_000;

#[derive(Debug, Clone)]
struct Row {
    event_time_ms: u64,
    mid: f64,
    features: Vec<f64>,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let mut args = std::env::args().skip(1);
    let input = PathBuf::from(args.next().context("usage: replay <events.parquet> <out.parquet>")?);
    let output = PathBuf::from(args.next().context("missing output path")?);

    tracing::info!(?input, ?output, "starting replay");

    let file = File::open(&input)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
        .with_batch_size(8192)
        .build()?;

    let mut book = OrderBook::new();
    let mut engine = FeatureEngine::new(TRADE_WINDOW_MS);
    let mut snapshot_anchored = false;
    let mut rows: Vec<Row> = Vec::new();
    let mut depth_count = 0u64;
    let mut trade_count = 0u64;
    let mut feature_count = 0u64;

    for batch in reader {
        let batch = batch?;
        process_batch(
            &batch,
            &mut book,
            &mut engine,
            &mut snapshot_anchored,
            &mut rows,
            &mut depth_count,
            &mut trade_count,
            &mut feature_count,
        )?;
    }

    tracing::info!(
        depth_count,
        trade_count,
        feature_count,
        rows = rows.len(),
        "replay done; labeling"
    );

    let labeled = label_rows(rows, LABEL_HORIZON_MS);
    tracing::info!(labeled = labeled.len(), "writing features parquet");

    write_features(&output, &labeled, FLUSH_EVERY)?;
    tracing::info!("done");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn process_batch(
    batch: &RecordBatch,
    book: &mut OrderBook,
    engine: &mut FeatureEngine,
    snapshot_anchored: &mut bool,
    rows: &mut Vec<Row>,
    depth_count: &mut u64,
    trade_count: &mut u64,
    feature_count: &mut u64,
) -> Result<()> {
    let kind = batch
        .column_by_name("kind")
        .context("kind")?
        .as_any()
        .downcast_ref::<StringArray>()
        .context("kind type")?;
    let event_time = batch
        .column_by_name("event_time_ms")
        .context("event_time_ms")?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .context("event_time_ms type")?;
    let first_id = batch
        .column_by_name("first_update_id")
        .context("first_update_id")?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .context("first_update_id type")?;
    let final_id = batch
        .column_by_name("final_update_id")
        .context("final_update_id")?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .context("final_update_id type")?;
    let trade_id = batch
        .column_by_name("trade_id")
        .context("trade_id")?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .context("trade_id type")?;
    let price = batch
        .column_by_name("price")
        .context("price")?
        .as_any()
        .downcast_ref::<StringArray>()
        .context("price type")?;
    let qty = batch
        .column_by_name("qty")
        .context("qty")?
        .as_any()
        .downcast_ref::<StringArray>()
        .context("qty type")?;
    let buyer_is_maker = batch
        .column_by_name("buyer_is_maker")
        .context("buyer_is_maker")?
        .as_any()
        .downcast_ref::<BooleanArray>()
        .context("buyer_is_maker type")?;
    let bids = batch
        .column_by_name("bids")
        .context("bids")?
        .as_any()
        .downcast_ref::<ListArray>()
        .context("bids type")?;
    let asks = batch
        .column_by_name("asks")
        .context("asks")?
        .as_any()
        .downcast_ref::<ListArray>()
        .context("asks type")?;

    for i in 0..batch.num_rows() {
        match kind.value(i) {
            "depth" => {
                let levels_b = read_levels(bids, i);
                let levels_a = read_levels(asks, i);
                let d = DepthUpdate {
                    event_type: "depthUpdate".into(),
                    event_time_ms: event_time.value(i),
                    symbol: "BTCUSDT".into(),
                    first_update_id: first_id.value(i),
                    final_update_id: final_id.value(i),
                    bids: levels_b,
                    asks: levels_a,
                };
                *depth_count += 1;

                if !*snapshot_anchored {
                    // Treat the first depth event as our pseudo-snapshot for replay.
                    let snap = feed::messages::SnapshotResponse {
                        last_update_id: d.final_update_id,
                        bids: d.bids.clone(),
                        asks: d.asks.clone(),
                    };
                    if book.apply_snapshot(snap).is_ok() {
                        *snapshot_anchored = true;
                    }
                    continue;
                }

                match book.apply_diff(&d) {
                    Ok(()) => {}
                    Err(book::BookError::StaleUpdate { .. }) => continue,
                    Err(book::BookError::SequenceGap { .. }) => {
                        // Re-anchor on gap.
                        let snap = feed::messages::SnapshotResponse {
                            last_update_id: d.final_update_id,
                            bids: d.bids.clone(),
                            asks: d.asks.clone(),
                        };
                        let _ = book.apply_snapshot(snap);
                        continue;
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "skipping bad diff");
                        continue;
                    }
                }

                if !book.is_initialized() {
                    continue;
                }
                let now_ms = d.event_time_ms;
                let v = engine.compute(book, now_ms);
                let mid = book.state().mid();
                rows.push(Row {
                    event_time_ms: now_ms,
                    mid,
                    features: v.as_slice().to_vec(),
                });
                *feature_count += 1;
            }
            "trade" => {
                if price.is_null(i) || qty.is_null(i) || buyer_is_maker.is_null(i) {
                    continue;
                }
                let t = Trade {
                    event_type: "trade".into(),
                    event_time_ms: event_time.value(i),
                    symbol: "BTCUSDT".into(),
                    trade_id: trade_id.value(i),
                    price: price.value(i).to_string(),
                    qty: qty.value(i).to_string(),
                    trade_time_ms: event_time.value(i),
                    buyer_is_maker: buyer_is_maker.value(i),
                };
                let now_ms = t.trade_time_ms;
                engine.trades_mut().push(t, now_ms);
                *trade_count += 1;
            }
            _ => {}
        }
    }
    Ok(())
}

fn read_levels(list: &ListArray, row: usize) -> Vec<PriceLevel> {
    if list.is_null(row) {
        return Vec::new();
    }
    let inner = list.value(row);
    let s = inner
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("level struct");
    let prices = s
        .column_by_name("price")
        .expect("price col")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("price str");
    let qtys = s
        .column_by_name("qty")
        .expect("qty col")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("qty str");
    let mut out = Vec::with_capacity(prices.len());
    for j in 0..prices.len() {
        let p: f64 = prices.value(j).parse().unwrap_or(0.0);
        let q: f64 = qtys.value(j).parse().unwrap_or(0.0);
        out.push(PriceLevel {
            price: OrderedFloat(p),
            qty: OrderedFloat(q),
        });
    }
    out
}

#[derive(Debug, Clone)]
struct LabeledRow {
    row: Row,
    label: i8,        // -1, 0, +1
    future_mid: f64,
}

fn label_rows(rows: Vec<Row>, horizon_ms: u64) -> Vec<LabeledRow> {
    let mut out = Vec::with_capacity(rows.len());
    let window: VecDeque<&Row> = VecDeque::new();
    // Two-pointer: for each row i, find earliest j with event_time >= rows[i] + horizon.
    let n = rows.len();
    let mut j = 0usize;
    for i in 0..n {
        let target = rows[i].event_time_ms.saturating_add(horizon_ms);
        if j < i {
            j = i;
        }
        while j < n && rows[j].event_time_ms < target {
            j += 1;
        }
        if j >= n {
            break;
        }
        let future_mid = rows[j].mid;
        let diff = future_mid - rows[i].mid;
        let label = if diff > 0.0 {
            1
        } else if diff < 0.0 {
            -1
        } else {
            0
        };
        out.push(LabeledRow {
            row: rows[i].clone(),
            label,
            future_mid,
        });
    }
    let _ = window;
    out
}

fn build_schema() -> Arc<Schema> {
    let mut fields: Vec<Field> = Vec::with_capacity(FEATURE_NAMES.len() + 4);
    fields.push(Field::new("event_time_ms", DataType::UInt64, false));
    fields.push(Field::new("mid_at_t", DataType::Float64, false));
    for name in FEATURE_NAMES {
        fields.push(Field::new(*name, DataType::Float64, false));
    }
    fields.push(Field::new("future_mid", DataType::Float64, false));
    fields.push(Field::new("label", DataType::Int8, false));
    Arc::new(Schema::new(fields))
}

fn write_features(path: &Path, rows: &[LabeledRow], flush_every: usize) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = File::create(path)?;
    let schema = build_schema();
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

    let mut chunk_start = 0usize;
    while chunk_start < rows.len() {
        let chunk_end = (chunk_start + flush_every).min(rows.len());
        let chunk = &rows[chunk_start..chunk_end];

        let mut event_time = UInt64Builder::with_capacity(chunk.len());
        let mut mid = Float64Builder::with_capacity(chunk.len());
        let mut feat_builders: Vec<Float64Builder> = (0..FEATURE_NAMES.len())
            .map(|_| Float64Builder::with_capacity(chunk.len()))
            .collect();
        let mut future_mid = Float64Builder::with_capacity(chunk.len());
        let mut label = Int8Builder::with_capacity(chunk.len());

        for r in chunk {
            event_time.append_value(r.row.event_time_ms);
            mid.append_value(r.row.mid);
            for (k, b) in feat_builders.iter_mut().enumerate() {
                b.append_value(r.row.features[k]);
            }
            future_mid.append_value(r.future_mid);
            label.append_value(r.label);
        }

        let mut cols: Vec<ArrayRef> = Vec::with_capacity(FEATURE_NAMES.len() + 4);
        cols.push(Arc::new(event_time.finish()) as ArrayRef);
        cols.push(Arc::new(mid.finish()) as ArrayRef);
        for mut b in feat_builders {
            cols.push(Arc::new(b.finish()) as ArrayRef);
        }
        cols.push(Arc::new(future_mid.finish()) as ArrayRef);
        cols.push(Arc::new(label.finish()) as ArrayRef);

        let batch = RecordBatch::try_new(schema.clone(), cols)?;
        writer.write(&batch)?;
        chunk_start = chunk_end;
    }
    writer.close()?;
    Ok(())
}