use std::error::Error;
use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;

use arrow_array::builder::StringBuilder;
use arrow_array::{ArrayRef, Date32Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;

use crate::attribution::Totals;
use crate::events::Catalog;
use crate::model::{Key, Keys, NO_CATEGORY};
use crate::sessions::Timeline;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

fn schema(fields: &[(&str, DataType, bool)]) -> SchemaRef {
    Arc::new(Schema::new(
        fields
            .iter()
            .map(|(name, kind, optional)| Field::new(*name, kind.clone(), *optional))
            .collect::<Vec<_>>(),
    ))
}

fn writer(path: &Path, schema: &SchemaRef) -> Result<ArrowWriter<File>> {
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_size(65_536)
        .build();
    Ok(ArrowWriter::try_new(
        File::create(path)?,
        schema.clone(),
        Some(props),
    )?)
}

fn text(builder: &mut StringBuilder, keys: &Keys, key: Key) {
    keys.with_bytes(key, |value| {
        builder.append_value(std::str::from_utf8(value).expect("input is UTF-8"))
    });
}

pub fn write(
    output: &Path,
    catalog: &Catalog,
    keys: &Keys,
    timelines: &[Timeline],
    totals: &Totals,
) -> Result<()> {
    let user_dir = output.join("user_daily");
    let campaign_dir = output.join("campaign_daily");
    fs::create_dir_all(&user_dir)?;
    fs::create_dir_all(&campaign_dir)?;

    let user_schema = schema(&[
        ("user_id", DataType::Utf8, false),
        ("day", DataType::Date32, false),
        ("sessions", DataType::Int64, false),
        ("events", DataType::Int64, false),
        ("revenue_cents", DataType::Int64, false),
        ("top_category", DataType::Utf8, true),
    ]);
    timelines
        .par_chunks(timelines.len().div_ceil(8))
        .enumerate()
        .try_for_each(|(shard, timelines)| -> Result<()> {
            let mut writer = writer(
                &user_dir.join(format!("part-{shard}.parquet")),
                &user_schema,
            )?;
            for timeline in timelines {
                for rows in timeline.user_days.chunks(65_536) {
                    let mut user_ids = StringBuilder::with_capacity(rows.len(), rows.len() * 12);
                    let mut categories = StringBuilder::with_capacity(rows.len(), rows.len() * 6);
                    for row in rows {
                        text(&mut user_ids, keys, row.person);
                        if row.category == NO_CATEGORY {
                            categories.append_null();
                        } else {
                            text(
                                &mut categories,
                                keys,
                                catalog.categories[row.category as usize],
                            );
                        }
                    }
                    let columns: Vec<ArrayRef> = vec![
                        Arc::new(user_ids.finish()),
                        Arc::new(Date32Array::from_iter_values(
                            rows.iter().map(|row| row.day),
                        )),
                        Arc::new(Int64Array::from_iter_values(
                            rows.iter().map(|row| row.sessions),
                        )),
                        Arc::new(Int64Array::from_iter_values(
                            rows.iter().map(|row| row.events),
                        )),
                        Arc::new(Int64Array::from_iter_values(
                            rows.iter().map(|row| row.revenue),
                        )),
                        Arc::new(categories.finish()),
                    ];
                    writer.write(&RecordBatch::try_new(user_schema.clone(), columns)?)?;
                }
            }
            writer.close()?;
            Ok(())
        })?;

    let campaign_schema = schema(&[
        ("campaign_id", DataType::Utf8, false),
        ("channel", DataType::Utf8, false),
        ("day", DataType::Date32, false),
        ("sessions", DataType::Int64, false),
        ("purchasing_sessions", DataType::Int64, false),
        ("revenue_cents", DataType::Int64, false),
        ("assisted_sessions", DataType::Int64, false),
    ]);
    let rows: Vec<_> = totals.rows().collect();
    let mut ids = StringBuilder::new();
    let mut channels = StringBuilder::new();
    for &(id, _, _) in &rows {
        let (id, channel) = catalog.campaign_names[id as usize];
        text(&mut ids, keys, id);
        text(&mut channels, keys, channel);
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(ids.finish()),
        Arc::new(channels.finish()),
        Arc::new(Date32Array::from_iter_values(rows.iter().map(|row| row.1))),
        Arc::new(Int64Array::from_iter_values(
            rows.iter().map(|row| row.2.sessions),
        )),
        Arc::new(Int64Array::from_iter_values(
            rows.iter().map(|row| row.2.purchasing),
        )),
        Arc::new(Int64Array::from_iter_values(
            rows.iter().map(|row| row.2.revenue),
        )),
        Arc::new(Int64Array::from_iter_values(
            rows.iter().map(|row| row.2.assists),
        )),
    ];
    let mut writer = writer(&campaign_dir.join("part-0.parquet"), &campaign_schema)?;
    writer.write(&RecordBatch::try_new(campaign_schema, columns)?)?;
    writer.close()?;
    Ok(())
}
