// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! End-to-end fleet lake verification: 10k property cells, one lake tick,
//! DuckDB aggregation over coalesced fleet Parquet.

use celld::analytics::{self, OutboxRow};
use celld::bucket::Bucket;
use celld::lake::{self, LakeConfig};
use futures_util::stream::{self, StreamExt};
use std::sync::Arc;
use std::time::Instant;
use tempfile::TempDir;

const NUM_PROPERTIES: usize = 10_000;
const EPOCH: u64 = 1;

fn expected_totals(num_properties: usize) -> (i64, i64, i64) {
    let mut count = 0i64;
    let mut sum_sqft = 0i64;
    for index in 0..num_properties {
        let spaces = (index % 100) + 1;
        count += spaces as i64;
        sum_sqft += (1..=spaces).map(|space| space as i64 * 100).sum::<i64>();
    }
    (count, sum_sqft, num_properties as i64)
}

fn property_cell_id(index: usize) -> String {
    format!("property-{index:05}")
}

fn spaces_for_property(index: usize) -> Vec<OutboxRow> {
    let property_id = property_cell_id(index);
    let num_spaces = (index % 100) + 1;
    (0..num_spaces)
        .map(|space| {
            let space_number = space + 1;
            let payload = serde_json::json!({
                "property_id": property_id,
                "space_id": format!("{property_id}-space-{space_number:03}"),
                "name": format!("Space {space_number}"),
                "sqft": space_number as i64 * 100,
            });
            OutboxRow {
                outbox_id: (space + 1) as i64,
                txid: 1,
                table_name: "spaces".into(),
                op: "upsert".into(),
                row_key: Some(format!("{property_id}-space-{space_number:03}")),
                payload: Some(serde_json::to_vec(&payload).unwrap()),
            }
        })
        .collect()
}

async fn seed_cell_deltas(bucket: &Bucket, index: usize) -> anyhow::Result<()> {
    let cell = property_cell_id(index);
    let rows = spaces_for_property(index);
    let own_key = format!("cells/{cell}/own.json");
    bucket
        .put(own_key.as_str(), r#"{"node":"test","epoch":1}"#)
        .await?;
    analytics::publish_delta(bucket, &cell, EPOCH, &rows, None, None).await?;
    Ok(())
}

#[tokio::test]
async fn fleet_lake_aggregates_ten_thousand_properties() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let bucket_url = format!("file://{}", temp.path().display());
    let bucket = Arc::new(Bucket::open(&bucket_url, None, "us-east-1", None, None)?);

    let seed_started = Instant::now();
    let mut seed = stream::iter(0..NUM_PROPERTIES)
        .map(|index| {
            let bucket = Arc::clone(&bucket);
            async move { seed_cell_deltas(bucket.as_ref(), index).await }
        })
        .buffer_unordered(64);
    while seed.next().await.transpose()?.is_some() {}
    eprintln!("seeded {NUM_PROPERTIES} cell deltas in {:?}", seed_started.elapsed());

    let config = LakeConfig {
        workers: 64,
        target_part_bytes: 128 * 1024 * 1024,
        retention_days: 90,
    };
    let merge_started = Instant::now();
    let report = lake::run_tick(bucket.as_ref(), &config).await?;
    eprintln!(
        "lake merge: {} rows from {} cells in {:?}",
        report.rows_merged,
        report.cells_merged,
        merge_started.elapsed()
    );

    let (expected_count, expected_sqft, expected_properties) = expected_totals(NUM_PROPERTIES);
    assert_eq!(report.rows_merged, expected_count as u64);
    assert_eq!(report.cells_merged, NUM_PROPERTIES);

    let parts = bucket.list("fleet/lake/spaces/").await?;
    let parquet_parts = parts
        .iter()
        .filter(|object| object.location.as_ref().ends_with(".parquet"))
        .count();
    assert!(
        parquet_parts < 100,
        "expected coalesced fleet parts, got {parquet_parts}"
    );

    let glob = format!("{}/fleet/lake/spaces/dt=*/part-*.parquet", temp.path().display());
    let query_started = Instant::now();
    let conn = duckdb::Connection::open_in_memory()?;
    let (count, sum_sqft, distinct_properties): (i64, i64, i64) = conn.query_row(
        &format!(
            "SELECT COUNT(*), COALESCE(SUM(sqft), 0), COUNT(DISTINCT property_id) \
             FROM read_parquet('{glob}')"
        ),
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    eprintln!("duckdb aggregation in {:?}", query_started.elapsed());
    assert_eq!(count, expected_count);
    assert_eq!(sum_sqft, expected_sqft);
    assert_eq!(distinct_properties, expected_properties);

    Ok(())
}
