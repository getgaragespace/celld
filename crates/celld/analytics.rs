// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Per-cell analytics ingest: outbox envelope, delta Parquet, and HEAD protocol.
//!
//! The replicator (or a test harness) drains `_celld_analytics_outbox` rows,
//! encodes them as Parquet under `cells/<cell>/analytics/e<epoch>/`, and
//! advances a JSON `HEAD` with conditional CAS. The fleet lake builder reads
//! those deltas; DuckDB should never scan this prefix at query time.

use crate::bucket::Bucket;
use anyhow::{anyhow, Context};
use bytes::Bytes;
use parquet::record::Field;
use parquet::record::Row;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

pub const OUTBOX_TABLE: &str = "_celld_analytics_outbox";

/// Per-class analytics opt-in from deploy manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnalyticsConfig {
    pub enabled: bool,
}

/// Coalescing limits for outbox drain batches.
#[derive(Debug, Clone, Copy)]
pub struct AnalyticsBatchConfig {
    pub max_txids: u64,
    pub max_ms: u64,
}

impl AnalyticsBatchConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        let max_txids = crate::env_vars::positive::<u64>("CELLD_ANALYTICS_BATCH_TXIDS")?
            .unwrap_or(64)
            .max(1);
        let max_ms = crate::env_vars::positive::<u64>("CELLD_ANALYTICS_BATCH_MS")?
            .unwrap_or(250)
            .max(1);
        Ok(Self { max_txids, max_ms })
    }
}

/// Whether a pending batch should be published now.
pub fn should_flush_batch(
    config: &AnalyticsBatchConfig,
    pending_rows: usize,
    span_txids: u64,
    pending_since_ms: u64,
    now_ms: u64,
    force: bool,
) -> bool {
    if pending_rows == 0 {
        return false;
    }
    force
        || span_txids >= config.max_txids
        || now_ms.saturating_sub(pending_since_ms) >= config.max_ms
}

fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn open_cell_db(
    db_path: &Path,
    vfs: Option<&str>,
    flags: rusqlite::OpenFlags,
) -> rusqlite::Result<rusqlite::Connection> {
    match vfs {
        Some(vfs) => rusqlite::Connection::open_with_flags_and_vfs(db_path, flags, vfs),
        None => rusqlite::Connection::open_with_flags(db_path, flags),
    }
}

/// Read unpublished outbox rows and stamp each with `durable_txid`.
pub fn read_unpublished_outbox(
    db_path: &Path,
    vfs: Option<&str>,
    durable_txid: i64,
    limit: usize,
) -> anyhow::Result<Vec<OutboxRow>> {
    let conn = open_cell_db(
        db_path,
        vfs,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("open {} for analytics outbox read", db_path.display()))?;
    let mut statement = conn.prepare(
        "SELECT id, table_name, op, row_key, payload \
         FROM _celld_analytics_outbox \
         WHERE published = 0 \
         ORDER BY id ASC \
         LIMIT ?1",
    )?;
    let rows = statement
        .query_map([limit as i64], |row| {
            let payload: Option<Vec<u8>> = match row.get_ref(4)? {
                rusqlite::types::ValueRef::Null => None,
                rusqlite::types::ValueRef::Blob(bytes) => Some(bytes.to_vec()),
                rusqlite::types::ValueRef::Text(bytes) => Some(bytes.to_vec()),
                other => {
                    return Err(rusqlite::Error::InvalidColumnType(
                        4,
                        "payload".into(),
                        other.data_type(),
                    ))
                }
            };
            Ok(OutboxRow {
                outbox_id: row.get(0)?,
                txid: durable_txid,
                table_name: row.get(1)?,
                op: row.get(2)?,
                row_key: row.get(3)?,
                payload,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Mark drained outbox rows published after a successful HEAD advance.
pub fn mark_outbox_published(
    db_path: &Path,
    vfs: Option<&str>,
    outbox_ids: &[i64],
) -> anyhow::Result<()> {
    if outbox_ids.is_empty() {
        return Ok(());
    }
    let conn = open_cell_db(db_path, vfs, rusqlite::OpenFlags::default())
        .with_context(|| format!("open {} for analytics outbox mark", db_path.display()))?;
    for id in outbox_ids {
        conn.execute(
            "UPDATE _celld_analytics_outbox SET published = 1 WHERE id = ?1",
            [*id],
        )?;
    }
    Ok(())
}

/// Drain unpublished outbox rows to the cell analytics prefix.
#[allow(clippy::too_many_arguments)]
pub async fn drain_outbox(
    bucket: &Bucket,
    cell: &str,
    epoch: u64,
    db_path: &Path,
    vfs: Option<&str>,
    durable_txid: i64,
    batch: &AnalyticsBatchConfig,
    pending_since_ms: u64,
    force: bool,
    prior_head: Option<&AnalyticsHead>,
    prior_token: Option<&str>,
) -> anyhow::Result<Option<(AnalyticsHead, String)>> {
    let limit = batch.max_txids.saturating_mul(1024) as usize;
    let rows = read_unpublished_outbox(db_path, vfs, durable_txid, limit)?;
    if rows.is_empty() {
        return Ok(None);
    }
    let min_txid = rows.iter().map(|row| row.txid).min().unwrap_or(durable_txid);
    let max_txid = rows.iter().map(|row| row.txid).max().unwrap_or(durable_txid);
    let span = (max_txid - min_txid + 1).max(1) as u64;
    if !should_flush_batch(batch, rows.len(), span, pending_since_ms, wall_ms(), force) {
        return Ok(None);
    }
    let (head, token) = publish_delta(bucket, cell, epoch, &rows, prior_head, prior_token).await?;
    let ids: Vec<i64> = rows.iter().map(|row| row.outbox_id).collect();
    mark_outbox_published(db_path, vfs, &ids)?;
    Ok(Some((head, token)))
}

/// Classes with analytics enabled, parsed from wrangler config.
pub fn analytics_classes_from_config(object: &serde_json::Map<String, serde_json::Value>) -> BTreeSet<String> {
    let mut enabled = BTreeSet::new();
    if let Some(map) = object.get("analytics").and_then(serde_json::Value::as_object) {
        for (class, config) in map {
            if config
                .get("enabled")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false)
            {
                enabled.insert(class.clone());
            }
        }
    }
    for binding in object
        .get("durable_objects")
        .and_then(|value| value.get("bindings"))
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
    {
        let Some(class) = binding.get("class_name").and_then(serde_json::Value::as_str) else {
            continue;
        };
        if binding
            .get("analytics")
            .and_then(|value| value.get("enabled"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
        {
            enabled.insert(class.to_string());
        }
    }
    enabled
}

const DELTA_MESSAGE_TYPE: &str = "
message celld_analytics_delta {
  required int64 outbox_id;
  required int64 txid;
  required binary table_name (STRING);
  required binary op (STRING);
  optional binary row_key (STRING);
  optional binary payload (STRING);
}";

/// Wire format for `cells/<cell>/analytics/e<epoch>/HEAD`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AnalyticsHead {
    pub epoch: u64,
    pub txid: i64,
    pub deltas: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base: Option<String>,
}

/// One drained outbox row, before or after Parquet encoding.
#[derive(Debug, Clone)]
pub struct OutboxRow {
    pub outbox_id: i64,
    pub txid: i64,
    pub table_name: String,
    pub op: String,
    pub row_key: Option<String>,
    pub payload: Option<Vec<u8>>,
}

pub fn head_key(cell: &str, epoch: u64) -> String {
    format!("cells/{cell}/analytics/e{epoch}/HEAD")
}

pub fn delta_object_name(min_txid: i64, max_txid: i64) -> String {
    format!("delta-{min_txid}-{max_txid}.parquet")
}

pub fn delta_key(cell: &str, epoch: u64, min_txid: i64, max_txid: i64) -> String {
    format!(
        "cells/{cell}/analytics/e{epoch}/{}",
        delta_object_name(min_txid, max_txid)
    )
}

pub fn encode_delta_parquet(rows: &[OutboxRow]) -> anyhow::Result<Vec<u8>> {
    use parquet::basic::Compression;
    use parquet::basic::ZstdLevel;
    use parquet::data_type::ByteArray;
    use parquet::data_type::ByteArrayType;
    use parquet::data_type::Int64Type;
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;
    use std::sync::Arc;

    if rows.is_empty() {
        bail_empty_delta()?;
    }

    let schema = Arc::new(parse_message_type(DELTA_MESSAGE_TYPE)?);
    let properties = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .build(),
    );
    let mut writer = SerializedFileWriter::new(Vec::new(), schema, properties)?;
    let mut group = writer.next_row_group()?;

    macro_rules! column {
        ($type:ty, req $values:expr) => {{
            let mut column = group.next_column()?.expect("schema column");
            column.typed::<$type>().write_batch(&$values, None, None)?;
            column.close()?;
        }};
        ($type:ty, opt $values:expr) => {{
            let mut column = group.next_column()?.expect("schema column");
            let options = $values;
            let levels: Vec<i16> = options.iter().map(|value| value.is_some() as i16).collect();
            let values: Vec<_> = options.into_iter().flatten().collect();
            column
                .typed::<$type>()
                .write_batch(&values, Some(&levels), None)?;
            column.close()?;
        }};
    }

    let text = |value: &str| ByteArray::from(value.as_bytes().to_vec());
    let each = rows.iter();

    column!(Int64Type, req each.clone().map(|row| row.outbox_id).collect::<Vec<_>>());
    column!(Int64Type, req each.clone().map(|row| row.txid).collect::<Vec<_>>());
    column!(
        ByteArrayType,
        req each
            .clone()
            .map(|row| text(&row.table_name))
            .collect::<Vec<_>>()
    );
    column!(
        ByteArrayType,
        req each.clone().map(|row| text(&row.op)).collect::<Vec<_>>()
    );
    column!(
        ByteArrayType,
        opt each
            .clone()
            .map(|row| row.row_key.as_deref().map(text))
            .collect::<Vec<_>>()
    );
    column!(
        ByteArrayType,
        opt each
            .clone()
            .map(|row| row.payload.as_deref().map(|value| text(std::str::from_utf8(value).unwrap_or_default())))
            .collect::<Vec<_>>()
    );

    group.close()?;
    Ok(writer.into_inner()?)
}

pub fn decode_delta_parquet(body: &[u8]) -> anyhow::Result<Vec<OutboxRow>> {
    use parquet::file::reader::FileReader;
    use parquet::file::reader::SerializedFileReader;

    let reader = SerializedFileReader::new(Bytes::copy_from_slice(body))?;
    let mut rows = Vec::new();
    for row in reader.get_row_iter(None)? {
        let row = row?;
        rows.push(parse_delta_row(&row)?);
    }
    Ok(rows)
}

fn parse_delta_row(row: &Row) -> anyhow::Result<OutboxRow> {
    let field = |name: &str| -> anyhow::Result<&Field> {
        row.get_column_iter()
            .find(|(column, _)| *column == name)
            .map(|(_, field)| field)
            .ok_or_else(|| anyhow!("delta row missing column {name}"))
    };
    let i64 = |name: &str| -> anyhow::Result<i64> {
        match field(name)? {
            Field::Long(value) => Ok(*value),
            other => Err(anyhow!("{name}: expected int64, got {other:?}")),
        }
    };
    let text = |name: &str| -> anyhow::Result<String> {
        match field(name)? {
            Field::Str(value) => Ok(value.clone()),
            other => Err(anyhow!("{name}: expected string, got {other:?}")),
        }
    };
    let opt_text = |name: &str| -> anyhow::Result<Option<String>> {
        match field(name)? {
            Field::Str(value) => Ok(Some(value.clone())),
            Field::Null => Ok(None),
            other => Err(anyhow!("{name}: expected string, got {other:?}")),
        }
    };
    let opt_bytes = |name: &str| -> anyhow::Result<Option<Vec<u8>>> {
        match field(name)? {
            Field::Bytes(value) => Ok(Some(value.data().to_vec())),
            Field::Str(value) => Ok(Some(value.as_bytes().to_vec())),
            Field::Null => Ok(None),
            other => Err(anyhow!("{name}: expected bytes, got {other:?}")),
        }
    };

    Ok(OutboxRow {
        outbox_id: i64("outbox_id")?,
        txid: i64("txid")?,
        table_name: text("table_name")?,
        op: text("op")?,
        row_key: opt_text("row_key")?,
        payload: opt_bytes("payload")?,
    })
}

fn bail_empty_delta() -> anyhow::Result<()> {
    Err(anyhow!("analytics delta must contain at least one row"))
}

/// Publish one delta object and advance `HEAD` with optional CAS on the prior token.
pub async fn publish_delta(
    bucket: &Bucket,
    cell: &str,
    epoch: u64,
    rows: &[OutboxRow],
    prior_head: Option<&AnalyticsHead>,
    prior_token: Option<&str>,
) -> anyhow::Result<(AnalyticsHead, String)> {
    let min_txid = rows
        .iter()
        .map(|row| row.txid)
        .min()
        .context("empty delta")?;
    let max_txid = rows
        .iter()
        .map(|row| row.txid)
        .max()
        .context("empty delta")?;
    let delta_name = delta_object_name(min_txid, max_txid);
    let parquet = encode_delta_parquet(rows)?;
    bucket
        .put(&delta_key(cell, epoch, min_txid, max_txid), parquet)
        .await?;

    let mut deltas = prior_head
        .map(|head| head.deltas.clone())
        .unwrap_or_default();
    if !deltas.iter().any(|name| name == &delta_name) {
        deltas.push(delta_name);
    }
    let head = AnalyticsHead {
        epoch,
        txid: max_txid,
        deltas,
        base: prior_head.and_then(|head| head.base.clone()),
    };
    let body = serde_json::to_vec(&head)?;
    let key = head_key(cell, epoch);
    let token = bucket
        .put_cas(&key, body, prior_token)
        .await?
        .ok_or_else(|| anyhow!("analytics HEAD CAS rejected for {cell} epoch {epoch}"))?;
    Ok((head, token))
}

pub async fn read_head(bucket: &Bucket, cell: &str, epoch: u64) -> anyhow::Result<Option<AnalyticsHead>> {
    let key = head_key(cell, epoch);
    let Some((body, _token)) = bucket.get(&key).await? else {
        return Ok(None);
    };
    let head = serde_json::from_slice(&body).context("parse analytics HEAD")?;
    Ok(Some(head))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delta_parquet_roundtrips() {
        let rows = vec![OutboxRow {
            outbox_id: 1,
            txid: 42,
            table_name: "spaces".into(),
            op: "upsert".into(),
            row_key: Some("s-1".into()),
            payload: Some(br#"{"sqft":100}"#.to_vec()),
        }];
        let encoded = encode_delta_parquet(&rows).unwrap();
        let decoded = decode_delta_parquet(&encoded).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].outbox_id, 1);
        assert_eq!(decoded[0].table_name, "spaces");
        assert_eq!(
            decoded[0].payload.as_deref(),
            Some(br#"{"sqft":100}"#.as_ref())
        );
    }
}
