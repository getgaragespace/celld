// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! End-to-end analytics write path: outbox insert → replicator drain → lake merge.

use celld::bucket::Bucket;
use celld::lake::{self, LakeConfig};
use celld::replication::{ActivationOptions, SyncWait};
use celld::runtime::Replication;
use tempfile::TempDir;

const CLASS: &str = "Property";
const EPOCH: u64 = 1;

fn cell_id(index: usize) -> String {
    format!("{CLASS}:property-{index:03}")
}

async fn seed_cell(
    replication: &Replication,
    watch: &std::path::Path,
    index: usize,
) -> anyhow::Result<()> {
    let cell = cell_id(index);
    let ltx = replication.ltx();
    ltx.activate(ActivationOptions {
        cell: &cell,
        epoch: EPOCH,
        fresh: true,
        took_over: false,
        resume_local: false,
        prior: None,
    })
    .await?;
    let payload = serde_json::json!({
        "property_id": cell,
        "space_id": format!("{cell}-space-001"),
        "name": "Space 1",
        "sqft": 100,
    });
    let payload_sql = payload.to_string().replace('\'', "''");
    ltx.exec_cell_sql(
        &cell,
        EPOCH,
        &format!(
            "CREATE TABLE IF NOT EXISTS _celld_analytics_outbox \
             (id INTEGER PRIMARY KEY AUTOINCREMENT, \
              table_name TEXT NOT NULL, \
              op TEXT NOT NULL, \
              row_key TEXT, \
              payload BLOB, \
              published INTEGER NOT NULL DEFAULT 0); \
             INSERT INTO _celld_analytics_outbox \
             (table_name, op, row_key, payload) \
             VALUES ('spaces', 'upsert', '{cell}-space-001', '{payload_sql}'); \
             CREATE TABLE IF NOT EXISTS spaces (id TEXT PRIMARY KEY, sqft INTEGER); \
             INSERT INTO spaces (id, sqft) VALUES ('{cell}-space-001', 100);"
        ),
    )
    .await?;
    let db_path = watch
        .join(&cell)
        .join("ltx")
        .join(format!("e{EPOCH}"))
        .join("db.sqlite");
    let pending =
        celld::analytics::read_unpublished_outbox(&db_path, None, 1, 10).map_err(|error| {
            anyhow::anyhow!("read outbox before sync: {error:#}")
        })?;
    anyhow::ensure!(!pending.is_empty(), "outbox empty before sync");
    let wait = ltx
        .sync_wait(&cell, EPOCH, std::time::Duration::from_secs(10))
        .await;
    anyhow::ensure!(
        matches!(wait, SyncWait::Durable),
        "analytics drain did not sync: {wait:?}"
    );
    Ok(())
}

#[tokio::test]
async fn fleet_analytics_write_path_ten_cells() -> anyhow::Result<()> {
    let root = TempDir::new()?;
    let watch = root.path().join("watch");
    let bucket_url = format!("file://{}", root.path().display());
    let bucket = Bucket::open(&bucket_url, None, "us-east-1", None, None)?;
    let replication = Replication::start(
        bucket.clone(),
        &watch,
        None,
        "us-east-1".to_string(),
        None,
    )?;
    replication.configure_analytics(&[CLASS.to_string()]);
    replication.ltx().set_analytics_ack_for_test(true);

    for index in 0..10 {
        seed_cell(&replication, &watch, index).await?;
    }

    for index in 0..10 {
        let cell = cell_id(index);
        let head = celld::analytics::read_head(&bucket, &cell, EPOCH).await?;
        anyhow::ensure!(head.is_some(), "missing analytics HEAD for {cell}");
    }

    let config = LakeConfig {
        workers: 4,
        target_part_bytes: 128 * 1024 * 1024,
        retention_days: 90,
    };
    let report = lake::run_tick(&bucket, &config).await?;
    assert_eq!(report.rows_merged, 10);
    assert_eq!(report.cells_merged, 10);

    let glob = format!("{}/fleet/lake/spaces/dt=*/part-*.parquet", root.path().display());
    let conn = duckdb::Connection::open_in_memory()?;
    let (count, sum_sqft): (i64, i64) = conn.query_row(
        &format!(
            "SELECT COUNT(*), COALESCE(SUM(sqft), 0) \
             FROM read_parquet('{glob}')"
        ),
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert_eq!(count, 10);
    assert_eq!(sum_sqft, 1000);

    Ok(())
}
