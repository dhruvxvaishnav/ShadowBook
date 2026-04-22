//! Parquet writer for periodic book snapshots (for parity/debug).

use anyhow::Result;
use arrow::array::{ArrayRef, Float64Builder, UInt64Builder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use book::BookState;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

const FLUSH_EVERY: usize = 500;

pub struct SnapshotWriter {
    writer: ArrowWriter<File>,
    schema: Arc<Schema>,
    local_ns: UInt64Builder,
    event_time_ms: UInt64Builder,
    last_update_id: UInt64Builder,
    best_bid: Float64Builder,
    best_bid_qty: Float64Builder,
    best_ask: Float64Builder,
    best_ask_qty: Float64Builder,
    microprice: Float64Builder,
    row_count: usize,
}

fn make_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("local_ns", DataType::UInt64, false),
        Field::new("event_time_ms", DataType::UInt64, false),
        Field::new("last_update_id", DataType::UInt64, false),
        Field::new("best_bid", DataType::Float64, false),
        Field::new("best_bid_qty", DataType::Float64, false),
        Field::new("best_ask", DataType::Float64, false),
        Field::new("best_ask_qty", DataType::Float64, false),
        Field::new("microprice", DataType::Float64, false),
    ]))
}

impl SnapshotWriter {
    pub fn create(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = File::create(path)?;
        let schema = make_schema();
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
            .build();
        let writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;
        Ok(Self {
            writer,
            schema,
            local_ns: UInt64Builder::new(),
            event_time_ms: UInt64Builder::new(),
            last_update_id: UInt64Builder::new(),
            best_bid: Float64Builder::new(),
            best_bid_qty: Float64Builder::new(),
            best_ask: Float64Builder::new(),
            best_ask_qty: Float64Builder::new(),
            microprice: Float64Builder::new(),
            row_count: 0,
        })
    }

    pub fn write_state(&mut self, state: &BookState, local_ns: u64) -> Result<()> {
        self.local_ns.append_value(local_ns);
        self.event_time_ms.append_value(state.event_time_ms);
        self.last_update_id.append_value(state.last_update_id);
        self.best_bid.append_value(state.best_bid);
        self.best_bid_qty.append_value(state.best_bid_qty);
        self.best_ask.append_value(state.best_ask);
        self.best_ask_qty.append_value(state.best_ask_qty);
        self.microprice.append_value(state.microprice());
        self.row_count += 1;
        if self.row_count >= FLUSH_EVERY {
            self.flush()?;
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        if self.row_count == 0 {
            return Ok(());
        }
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(self.local_ns.finish()) as ArrayRef,
                Arc::new(self.event_time_ms.finish()) as ArrayRef,
                Arc::new(self.last_update_id.finish()) as ArrayRef,
                Arc::new(self.best_bid.finish()) as ArrayRef,
                Arc::new(self.best_bid_qty.finish()) as ArrayRef,
                Arc::new(self.best_ask.finish()) as ArrayRef,
                Arc::new(self.best_ask_qty.finish()) as ArrayRef,
                Arc::new(self.microprice.finish()) as ArrayRef,
            ],
        )?;
        self.writer.write(&batch)?;
        self.row_count = 0;
        Ok(())
    }

    pub fn close(mut self) -> Result<()> {
        self.flush()?;
        self.writer.close()?;
        Ok(())
    }
}
