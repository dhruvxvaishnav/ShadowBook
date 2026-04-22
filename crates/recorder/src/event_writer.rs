//! Parquet writer for raw market events (depth diffs + trades).
//!
//! Schema v1 — any change bumps the version and requires a new training set.

use anyhow::Result;
use arrow::array::{
    ArrayRef, ListBuilder, StringBuilder, StructBuilder, UInt64Array, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use feed::messages::MarketEvent;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

const FLUSH_EVERY: usize = 2_000;

/// Flat-row schema:
/// - `kind`: "depth" | "trade"
/// - `local_recv_ns`: u64 — local monotonic receive time in ns since epoch
/// - `event_time_ms`: u64 — exchange event time
/// - `first_update_id`, `final_update_id`: depth only (0 for trade)
/// - `trade_id`: trade only (0 for depth)
/// - `price`, `qty`, `buyer_is_maker`: trade only
/// - `bids`, `asks`: list<struct<price:utf8, qty:utf8>> for depth
pub struct EventWriter {
    writer: ArrowWriter<File>,
    schema: Arc<Schema>,
    // Column builders
    kind: StringBuilder,
    local_recv_ns: UInt64Builder,
    event_time_ms: UInt64Builder,
    first_update_id: UInt64Builder,
    final_update_id: UInt64Builder,
    trade_id: UInt64Builder,
    price: StringBuilder,
    qty: StringBuilder,
    buyer_is_maker: arrow::array::BooleanBuilder,
    bids: ListBuilder<StructBuilder>,
    asks: ListBuilder<StructBuilder>,
    row_count: usize,
}

fn level_struct_fields() -> Vec<Field> {
    vec![
        Field::new("price", DataType::Utf8, false),
        Field::new("qty", DataType::Utf8, false),
    ]
}

fn level_list_field(name: &str) -> Field {
    Field::new(
        name,
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(level_struct_fields().into()),
            true,
        ))),
        true,
    )
}

fn make_level_builder() -> ListBuilder<StructBuilder> {
    let fields = level_struct_fields();
    let builders: Vec<Box<dyn arrow::array::ArrayBuilder>> = vec![
        Box::new(StringBuilder::new()),
        Box::new(StringBuilder::new()),
    ];
    ListBuilder::new(StructBuilder::new(fields, builders))
}

fn make_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("kind", DataType::Utf8, false),
        Field::new("local_recv_ns", DataType::UInt64, false),
        Field::new("event_time_ms", DataType::UInt64, false),
        Field::new("first_update_id", DataType::UInt64, false),
        Field::new("final_update_id", DataType::UInt64, false),
        Field::new("trade_id", DataType::UInt64, false),
        Field::new("price", DataType::Utf8, true),
        Field::new("qty", DataType::Utf8, true),
        Field::new("buyer_is_maker", DataType::Boolean, true),
        level_list_field("bids"),
        level_list_field("asks"),
    ]))
}

impl EventWriter {
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
        tracing::info!(?path, "event writer opened");
        Ok(Self {
            writer,
            schema,
            kind: StringBuilder::new(),
            local_recv_ns: UInt64Builder::new(),
            event_time_ms: UInt64Builder::new(),
            first_update_id: UInt64Builder::new(),
            final_update_id: UInt64Builder::new(),
            trade_id: UInt64Builder::new(),
            price: StringBuilder::new(),
            qty: StringBuilder::new(),
            buyer_is_maker: arrow::array::BooleanBuilder::new(),
            bids: make_level_builder(),
            asks: make_level_builder(),
            row_count: 0,
        })
    }

    pub fn write_event(&mut self, ev: &MarketEvent, local_recv_ns: u64) -> Result<()> {
        self.local_recv_ns.append_value(local_recv_ns);
        self.event_time_ms.append_value(ev.event_time_ms());

        match ev {
            MarketEvent::Depth(d) => {
                self.kind.append_value("depth");
                self.first_update_id.append_value(d.first_update_id);
                self.final_update_id.append_value(d.final_update_id);
                self.trade_id.append_value(0);
                self.price.append_null();
                self.qty.append_null();
                self.buyer_is_maker.append_null();
                append_levels(&mut self.bids, &d.bids);
                append_levels(&mut self.asks, &d.asks);
            }
            MarketEvent::Trade(t) => {
                self.kind.append_value("trade");
                self.first_update_id.append_value(0);
                self.final_update_id.append_value(0);
                self.trade_id.append_value(t.trade_id);
                self.price.append_value(&t.price);
                self.qty.append_value(&t.qty);
                self.buyer_is_maker.append_value(t.buyer_is_maker);
                // Empty lists for trades
                self.bids.append(true);
                self.asks.append(true);
            }
        }

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
                Arc::new(self.kind.finish()) as ArrayRef,
                Arc::new(self.local_recv_ns.finish()) as ArrayRef,
                Arc::new(self.event_time_ms.finish()) as ArrayRef,
                Arc::new(self.first_update_id.finish()) as ArrayRef,
                Arc::new(self.final_update_id.finish()) as ArrayRef,
                Arc::new(self.trade_id.finish()) as ArrayRef,
                Arc::new(self.price.finish()) as ArrayRef,
                Arc::new(self.qty.finish()) as ArrayRef,
                Arc::new(self.buyer_is_maker.finish()) as ArrayRef,
                Arc::new(self.bids.finish()) as ArrayRef,
                Arc::new(self.asks.finish()) as ArrayRef,
            ],
        )?;
        self.writer.write(&batch)?;
        tracing::debug!(rows = self.row_count, "parquet batch written");
        self.row_count = 0;
        Ok(())
    }

    pub fn close(mut self) -> Result<()> {
        self.flush()?;
        self.writer.close()?;
        tracing::info!("event writer closed");
        Ok(())
    }
}

fn append_levels(builder: &mut ListBuilder<StructBuilder>, levels: &[feed::messages::PriceLevel]) {
    let values = builder.values();
    for lvl in levels {
        values
            .field_builder::<StringBuilder>(0)
            .expect("price builder")
            .append_value(lvl.price.0.to_string());
        values
            .field_builder::<StringBuilder>(1)
            .expect("qty builder")
            .append_value(lvl.qty.0.to_string());
        values.append(true);
    }
    builder.append(true);
}

// Silences unused warning for UInt64Array in some configurations.
#[allow(dead_code)]
fn _force_import() -> Option<UInt64Array> {
    None
}
