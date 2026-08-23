// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Fleet analytics lake builder: merge per-cell deltas into query-shaped Parquet.
//!
//! Operators run `celld lake run` (or a supervisor loop) to materialize
//! `fleet/lake/<table>/dt=.../part-*.parquet` from the per-cell ingest stream.

use crate::analytics::{self, AnalyticsHead, OutboxRow};
use crate::bucket::Bucket;
use anyhow::{anyhow, Context};
use futures_util::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub const CHECKPOINT_KEY: &str = "fleet/lake/_checkpoint.json";
pub const BUILDER_KEY: &str = "fleet/lake/_builder.json";

#[derive(Debug, Clone)]
pub struct LakeConfig {
    pub workers: usize,
    pub target_part_bytes: usize,
    pub retention_days: u32,
}

impl LakeConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        let workers = crate::env_vars::positive::<usize>("CELLD_LAKE_WORKERS")?.unwrap_or(32);
        let target_part_mb = crate::env_vars::positive::<usize>("CELLD_LAKE_TARGET_PART_MB")?
            .unwrap_or(128);
        let retention_days = crate::env_vars::positive::<u32>("CELLD_LAKE_RETENTION_DAYS")?
            .unwrap_or(90);
        Ok(Self {
            workers: workers.max(1),
            target_part_bytes: target_part_mb.saturating_mul(1024 * 1024),
            retention_days,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Checkpoint {
    pub version: u32,
    #[serde(default)]
    pub cells: BTreeMap<String, CellCheckpoint>,
    pub updated_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CellCheckpoint {
    pub epoch: u64,
    pub txid: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableManifest {
    pub version: u32,
    pub table: String,
    pub files: Vec<ManifestFile>,
    #[serde(default)]
    pub partitions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestFile {
    pub path: String,
    pub rows: u64,
    pub bytes: u64,
    pub min_txid: i64,
    pub max_txid: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuilderStatus {
    pub started_ms: u64,
    pub finished_ms: u64,
    pub cells_scanned: usize,
    pub cells_merged: usize,
    pub rows_merged: u64,
    pub tables: Vec<String>,
}

#[derive(Debug, Default)]
pub struct RunReport {
    pub cells_scanned: usize,
    pub cells_merged: usize,
    pub rows_merged: u64,
    pub tables: Vec<String>,
}

#[derive(Debug, Clone)]
struct FleetRow {
    table: String,
    property_id: String,
    space_id: String,
    name: String,
    sqft: i64,
    cell: String,
    epoch: u64,
    txid: i64,
    merged_ms: u64,
}

pub async fn run_tick(bucket: &Bucket, config: &LakeConfig) -> anyhow::Result<RunReport> {
    let started_ms = wall_ms();
    let (checkpoint, checkpoint_token) = load_checkpoint(bucket).await?;
    let cell_ids = discover_cells(bucket).await?;
    let cells_scanned = cell_ids.len();
    let partition = utc_date_partition(wall_ms());
    let checkpoint_cells = Arc::new(checkpoint.cells.clone());
    let workers = config.workers;
    let bucket = bucket.clone();

    let mut merged_rows: Vec<FleetRow> = Vec::new();
    let mut cells_merged = 0usize;

    let mut stream = stream::iter(cell_ids.iter().cloned())
        .map(|cell| {
            let bucket = bucket.clone();
            let checkpoint_cells = Arc::clone(&checkpoint_cells);
            async move {
                merge_cell(&bucket, &cell, &checkpoint_cells)
                    .await
                    .map(|rows| (cell, rows))
            }
        })
        .buffer_unordered(workers);

    while let Some(result) = stream.next().await {
        let (_cell, rows) = result?;
        if !rows.is_empty() {
            cells_merged += 1;
            merged_rows.extend(rows);
        }
    }

    let rows_merged = merged_rows.len() as u64;
    let mut tables: BTreeSet<String> = BTreeSet::new();
    for row in &merged_rows {
        tables.insert(row.table.clone());
    }

    if !merged_rows.is_empty() {
        write_fleet_parts(&bucket, &partition, &merged_rows, config.target_part_bytes)
            .await?;
        for table in &tables {
            update_manifest(&bucket, table, &partition).await?;
        }
    }

    let mut next_checkpoint = checkpoint;
    next_checkpoint.version = 1;
    next_checkpoint.updated_ms = wall_ms();
    for cell in &cell_ids {
        if let Some(head) = latest_head(&bucket, cell).await? {
            let watermark = next_checkpoint
                .cells
                .get(cell)
                .filter(|entry| entry.epoch == head.epoch)
                .map(|entry| entry.txid)
                .unwrap_or(0);
            if head.txid > watermark {
                next_checkpoint.cells.insert(
                    cell.clone(),
                    CellCheckpoint {
                        epoch: head.epoch,
                        txid: head.txid,
                    },
                );
            }
        }
    }
    save_checkpoint(&bucket, &next_checkpoint, checkpoint_token.as_deref()).await?;

    let status = BuilderStatus {
        started_ms,
        finished_ms: wall_ms(),
        cells_scanned,
        cells_merged,
        rows_merged,
        tables: tables.iter().cloned().collect(),
    };
    bucket
        .put(BUILDER_KEY, serde_json::to_vec(&status)?)
        .await?;

    Ok(RunReport {
        cells_scanned: status.cells_scanned,
        cells_merged: status.cells_merged,
        rows_merged: status.rows_merged,
        tables: status.tables,
    })
}

async fn merge_cell(
    bucket: &Bucket,
    cell: &str,
    checkpoint_cells: &BTreeMap<String, CellCheckpoint>,
) -> anyhow::Result<Vec<FleetRow>> {
    let Some(head) = latest_head(bucket, cell).await? else {
        return Ok(Vec::new());
    };
    let watermark = checkpoint_cells
        .get(cell)
        .filter(|entry| entry.epoch == head.epoch)
        .map(|entry| entry.txid)
        .unwrap_or(0);
    if head.txid <= watermark {
        return Ok(Vec::new());
    }

    let merged_ms = wall_ms();
    let mut rows = Vec::new();
    let mut seen = HashSet::new();
    for delta_name in &head.deltas {
        let key = format!("cells/{cell}/analytics/e{}/{}", head.epoch, delta_name);
        let Some((body, _)) = bucket.get(&key).await? else {
            continue;
        };
        for outbox in analytics::decode_delta_parquet(&body)? {
            if outbox.txid <= watermark {
                continue;
            }
            let dedupe = format!("{cell}:{}:{}", outbox.outbox_id, outbox.txid);
            if !seen.insert(dedupe) {
                continue;
            }
            if let Some(fleet_row) = fleet_row_from_outbox(cell, head.epoch, merged_ms, &outbox)? {
                rows.push(fleet_row);
            }
        }
    }
    Ok(rows)
}

fn fleet_row_from_outbox(
    cell: &str,
    epoch: u64,
    merged_ms: u64,
    outbox: &OutboxRow,
) -> anyhow::Result<Option<FleetRow>> {
    if outbox.table_name != "spaces" {
        return Ok(None);
    }
    let payload = outbox
        .payload
        .as_deref()
        .ok_or_else(|| anyhow!("spaces outbox row missing payload"))?;
    let json: serde_json::Value = serde_json::from_slice(payload).context("spaces payload json")?;
    let property_id = json
        .get("property_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow!("spaces payload missing property_id"))?
        .to_string();
    let space_id = json
        .get("space_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow!("spaces payload missing space_id"))?
        .to_string();
    let name = json
        .get("name")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_string();
    let sqft = json
        .get("sqft")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| anyhow!("spaces payload missing sqft"))?;
    Ok(Some(FleetRow {
        table: "spaces".into(),
        property_id,
        space_id,
        name,
        sqft,
        cell: cell.to_string(),
        epoch,
        txid: outbox.txid,
        merged_ms,
    }))
}

async fn write_fleet_parts(
    bucket: &Bucket,
    partition: &str,
    rows: &[FleetRow],
    target_part_bytes: usize,
) -> anyhow::Result<()> {
    let mut offset = 0usize;
    let mut part = next_part_number(bucket, "spaces", partition).await?;
    while offset < rows.len() {
        let mut end = (offset + 65_536).min(rows.len());
        loop {
            let encoded = encode_spaces_part(&rows[offset..end])?;
            if encoded.len() <= target_part_bytes || end - offset == 1 {
                let path = fleet_part_path("spaces", partition, part);
                bucket.put(&path, encoded).await?;
                part += 1;
                offset = end;
                break;
            }
            end = offset + (end - offset) / 2;
        }
    }
    Ok(())
}

fn fleet_part_path(table: &str, partition: &str, part: u32) -> String {
    format!("fleet/lake/{table}/{partition}/part-{part:06}.parquet")
}

async fn next_part_number(bucket: &Bucket, table: &str, partition: &str) -> anyhow::Result<u32> {
    let prefix = format!("fleet/lake/{table}/{partition}/");
    let mut max = 0u32;
    for object in bucket.list(&prefix).await? {
        let name = object.location.as_ref();
        if let Some(stem) = name.strip_prefix(&prefix).and_then(|tail| tail.strip_suffix(".parquet"))
        {
            if let Some(number) = stem.strip_prefix("part-").and_then(|digits| digits.parse().ok())
            {
                max = max.max(number);
            }
        }
    }
    Ok(max + 1)
}

const SPACES_MESSAGE_TYPE: &str = "
message celld_fleet_spaces {
  required binary property_id (STRING);
  required binary space_id (STRING);
  required binary name (STRING);
  required int64 sqft;
  required binary _celld_cell (STRING);
  required int64 _celld_epoch;
  required int64 _celld_txid;
  required int64 _celld_merged_ms;
}";

fn encode_spaces_part(rows: &[FleetRow]) -> anyhow::Result<Vec<u8>> {
    use parquet::basic::Compression;
    use parquet::basic::ZstdLevel;
    use parquet::data_type::ByteArray;
    use parquet::data_type::ByteArrayType;
    use parquet::data_type::Int64Type;
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;
    use std::sync::Arc;

    let schema = Arc::new(parse_message_type(SPACES_MESSAGE_TYPE)?);
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
    }

    let text = |value: &str| ByteArray::from(value.as_bytes().to_vec());
    let each = rows.iter();

    column!(
        ByteArrayType,
        req each
            .clone()
            .map(|row| text(&row.property_id))
            .collect::<Vec<_>>()
    );
    column!(
        ByteArrayType,
        req each.clone().map(|row| text(&row.space_id)).collect::<Vec<_>>()
    );
    column!(
        ByteArrayType,
        req each.clone().map(|row| text(&row.name)).collect::<Vec<_>>()
    );
    column!(Int64Type, req each.clone().map(|row| row.sqft).collect::<Vec<_>>());
    column!(
        ByteArrayType,
        req each.clone().map(|row| text(&row.cell)).collect::<Vec<_>>()
    );
    column!(
        Int64Type,
        req each.clone().map(|row| row.epoch as i64).collect::<Vec<_>>()
    );
    column!(Int64Type, req each.clone().map(|row| row.txid).collect::<Vec<_>>());
    column!(
        Int64Type,
        req each.clone().map(|row| row.merged_ms as i64).collect::<Vec<_>>()
    );

    group.close()?;
    Ok(writer.into_inner()?)
}

async fn update_manifest(bucket: &Bucket, table: &str, partition: &str) -> anyhow::Result<()> {
    let prefix = format!("fleet/lake/{table}/{partition}/");
    let mut files = Vec::new();
    let mut partitions = BTreeSet::new();
    partitions.insert(partition.to_string());
    for object in bucket.list(&prefix).await? {
        let path = object.location.to_string();
        let bytes = bucket
            .get(&path)
            .await?
            .map(|(body, _)| body.len() as u64)
            .unwrap_or(0);
        files.push(ManifestFile {
            path,
            rows: 0,
            bytes,
            min_txid: 0,
            max_txid: 0,
        });
    }
    let manifest_key = format!("fleet/lake/{table}/_manifest.json");
    let mut manifest = load_manifest(bucket, table).await?;
    manifest.version = 1;
    manifest.table = table.to_string();
    manifest
        .files
        .retain(|file| !file.path.starts_with(&format!("fleet/lake/{table}/{partition}/")));
    manifest.files.extend(files);
    manifest.partitions = manifest
        .partitions
        .iter()
        .cloned()
        .chain(partitions)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    bucket
        .put(&manifest_key, serde_json::to_vec(&manifest)?)
        .await?;
    Ok(())
}

async fn load_manifest(bucket: &Bucket, table: &str) -> anyhow::Result<TableManifest> {
    let key = format!("fleet/lake/{table}/_manifest.json");
    let Ok(Some((body, _))) = bucket.get(&key).await else {
        return Ok(TableManifest {
            version: 1,
            table: table.to_string(),
            files: Vec::new(),
            partitions: Vec::new(),
        });
    };
    Ok(serde_json::from_slice(&body).context("parse table manifest")?)
}

async fn load_checkpoint(bucket: &Bucket) -> anyhow::Result<(Checkpoint, Option<String>)> {
    let Ok(Some((body, token))) = bucket.get(CHECKPOINT_KEY).await else {
        return Ok((
            Checkpoint {
                version: 1,
                cells: BTreeMap::new(),
                updated_ms: 0,
            },
            None,
        ));
    };
    let checkpoint = serde_json::from_slice(&body).context("parse lake checkpoint")?;
    Ok((checkpoint, Some(token)))
}

async fn save_checkpoint(
    bucket: &Bucket,
    checkpoint: &Checkpoint,
    token: Option<&str>,
) -> anyhow::Result<()> {
    let body = serde_json::to_vec(checkpoint)?;
    match bucket.put_cas(CHECKPOINT_KEY, body, token).await? {
        Some(_) => Ok(()),
        None => Err(anyhow!("lake checkpoint CAS rejected; retry the tick")),
    }
}

async fn discover_cells(bucket: &Bucket) -> anyhow::Result<Vec<String>> {
    let prefixes = bucket.common_prefixes("cells/").await?;
    Ok(prefixes
        .into_iter()
        .filter_map(|prefix| prefix.strip_prefix("cells/").map(str::to_string))
        .filter(|cell| !cell.is_empty())
        .collect())
}

async fn latest_head(bucket: &Bucket, cell: &str) -> anyhow::Result<Option<AnalyticsHead>> {
    let epochs = bucket
        .common_prefixes(&format!("cells/{cell}/analytics"))
        .await?;
    let mut best = None::<(u64, AnalyticsHead)>;
    for prefix in epochs {
        let Some(epoch_text) = prefix.rsplit('/').next() else {
            continue;
        };
        let Some(epoch_digits) = epoch_text.strip_prefix('e') else {
            continue;
        };
        let epoch: u64 = epoch_digits.parse().context("analytics epoch")?;
        if let Some(head) = analytics::read_head(bucket, cell, epoch).await? {
            match &best {
                Some((best_epoch, _)) if *best_epoch >= epoch => {}
                _ => best = Some((epoch, head)),
            }
        }
    }
    Ok(best.map(|(_, head)| head))
}

fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn utc_date_partition(now_ms: u64) -> String {
    let days = now_ms / 86_400_000;
    let (year, month, day) = civil_from_days(days as i64);
    format!("dt={year:04}-{month:02}-{day:02}")
}

fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    let year = y + if m <= 2 { 1 } else { 0 };
    (year as i32, m as u32, d as u32)
}

#[derive(Debug, Default)]
pub struct CompactReport {
    pub tables: usize,
    pub merged_parts: usize,
    pub output_parts: usize,
}

pub async fn compact_tick(bucket: &Bucket, config: &LakeConfig) -> anyhow::Result<CompactReport> {
    let mut report = CompactReport::default();
    let prefixes = bucket.common_prefixes("fleet/lake/").await?;
    for prefix in prefixes {
        let Some(table) = prefix
            .strip_prefix("fleet/lake/")
            .and_then(|tail| tail.strip_suffix('/'))
            .filter(|name| !name.starts_with('_'))
        else {
            continue;
        };
        let manifest = load_manifest(bucket, table).await?;
        if manifest.files.is_empty() {
            continue;
        }
        report.tables += 1;
        for partition in &manifest.partitions {
            let part_prefix = format!("fleet/lake/{table}/{partition}/");
            let mut parts = Vec::new();
            for object in bucket.list(&part_prefix).await? {
                let path = object.location.to_string();
                if !path.ends_with(".parquet") {
                    continue;
                }
                let bytes = bucket
                    .get(&path)
                    .await?
                    .map(|(body, _)| body.len())
                    .unwrap_or(0);
                parts.push((path, bytes));
            }
            if parts.len() <= 1 {
                continue;
            }
            let total_bytes: usize = parts.iter().map(|(_, bytes)| *bytes).sum();
            if total_bytes <= config.target_part_bytes && parts.len() < 8 {
                continue;
            }
            let mut rows = Vec::new();
            for (path, _) in &parts {
                let Some((body, _)) = bucket.get(path).await? else {
                    continue;
                };
                rows.extend(decode_spaces_part(&body)?);
            }
            if rows.is_empty() {
                continue;
            }
            let encoded = encode_spaces_part(&rows)?;
            let part = next_part_number(bucket, table, partition).await?;
            let merged_path = fleet_part_path(table, partition, part);
            bucket.put(&merged_path, encoded).await?;
            for (path, _) in &parts {
                bucket.delete(path).await?;
            }
            report.merged_parts += parts.len();
            report.output_parts += 1;
            update_manifest(bucket, table, partition).await?;
        }
    }
    Ok(report)
}

fn decode_spaces_part(body: &[u8]) -> anyhow::Result<Vec<FleetRow>> {
    use parquet::file::reader::FileReader;
    use parquet::file::reader::SerializedFileReader;

    let reader = SerializedFileReader::new(bytes::Bytes::copy_from_slice(body))?;
    let mut rows = Vec::new();
    for row in reader.get_row_iter(None)? {
        let row = row?;
        let field = |name: &str| -> anyhow::Result<&parquet::record::Field> {
            row.get_column_iter()
                .find(|(column, _)| *column == name)
                .map(|(_, field)| field)
                .ok_or_else(|| anyhow!("fleet row missing column {name}"))
        };
        let text = |name: &str| -> anyhow::Result<String> {
            match field(name)? {
                parquet::record::Field::Str(value) => Ok(value.clone()),
                other => Err(anyhow!("{name}: expected string, got {other:?}")),
            }
        };
        let i64 = |name: &str| -> anyhow::Result<i64> {
            match field(name)? {
                parquet::record::Field::Long(value) => Ok(*value),
                other => Err(anyhow!("{name}: expected int64, got {other:?}")),
            }
        };
        rows.push(FleetRow {
            table: "spaces".into(),
            property_id: text("property_id")?,
            space_id: text("space_id")?,
            name: text("name")?,
            sqft: i64("sqft")?,
            cell: text("_celld_cell")?,
            epoch: i64("_celld_epoch")? as u64,
            txid: i64("_celld_txid")?,
            merged_ms: i64("_celld_merged_ms")? as u64,
        });
    }
    Ok(rows)
}

pub async fn run_cli(arguments: Vec<String>) -> anyhow::Result<()> {
    let mut arguments = arguments;
    let subcommand = arguments.first().map(String::as_str).unwrap_or("help");
    match subcommand {
        "run" => {
            arguments.remove(0);
            let daemon = arguments.iter().any(|arg| arg == "--daemon");
            arguments.retain(|arg| arg != "--daemon");
            let bucket = bucket_from_cli(&arguments)?;
            let config = LakeConfig::from_env()?;
            if daemon {
                let interval = crate::env_vars::positive::<u64>("CELLD_LAKE_INTERVAL_S")?
                    .unwrap_or(30);
                loop {
                    let report = run_tick(&bucket, &config).await?;
                    tracing::info!(
                        cells_scanned = report.cells_scanned,
                        cells_merged = report.cells_merged,
                        rows_merged = report.rows_merged,
                        tables = ?report.tables,
                        "lake tick complete"
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(interval)).await;
                }
            } else {
                let report = run_tick(&bucket, &config).await?;
                crate::cli_output::Output::new(crate::cli_output::Format::Text).line(format_args!(
                    "merged {} rows from {} cells into {:?}",
                    report.rows_merged, report.cells_merged, report.tables
                ))?;
            }
        }
        "status" => {
            let bucket = bucket_from_cli(&arguments[1..])?;
            let (checkpoint, _) = load_checkpoint(&bucket).await?;
            let status = bucket.get(BUILDER_KEY).await?;
            crate::cli_output::Output::new(crate::cli_output::Format::Text)
                .line(format_args!("checkpoint cells: {}", checkpoint.cells.len()))?;
            if let Some((body, _)) = status {
                let status: BuilderStatus = serde_json::from_slice(&body)?;
                crate::cli_output::Output::new(crate::cli_output::Format::Text).line(format_args!(
                    "last run: {} cells scanned, {} merged, {} rows, tables {:?}",
                    status.cells_scanned,
                    status.cells_merged,
                    status.rows_merged,
                    status.tables
                ))?;
            } else {
                crate::cli_output::Output::new(crate::cli_output::Format::Text)
                    .line(format_args!("last run: none"))?;
            }
        }
        "compact" => {
            let bucket = bucket_from_cli(&arguments[1..])?;
            let config = LakeConfig::from_env()?;
            let report = compact_tick(&bucket, &config).await?;
            crate::cli_output::Output::new(crate::cli_output::Format::Text).line(format_args!(
                "compacted {} tables, merged {} parts into {}",
                report.tables, report.merged_parts, report.output_parts
            ))?;
        }
        _ => {
            print_lake_help()?;
        }
    }
    Ok(())
}

fn bucket_from_cli(arguments: &[String]) -> anyhow::Result<Bucket> {
    let mut bucket = None;
    let mut endpoint = None;
    let mut region = std::env::var("AWS_REGION")
        .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
        .unwrap_or_else(|_| "us-east-1".to_string());
    let mut index = 0usize;
    while index < arguments.len() {
        match arguments[index].as_str() {
            "--bucket" => {
                index += 1;
                bucket = Some(
                    arguments
                        .get(index)
                        .cloned()
                        .ok_or_else(|| anyhow!("--bucket requires a value"))?,
                );
            }
            "--endpoint" => {
                index += 1;
                endpoint = Some(
                    arguments
                        .get(index)
                        .cloned()
                        .ok_or_else(|| anyhow!("--endpoint requires a value"))?,
                );
            }
            "--region" => {
                index += 1;
                region = arguments
                    .get(index)
                    .cloned()
                    .ok_or_else(|| anyhow!("--region requires a value"))?;
            }
            other if other.starts_with("--bucket=") => {
                bucket = Some(other.trim_start_matches("--bucket=").to_string());
            }
            other if other.starts_with("--endpoint=") => {
                endpoint = Some(other.trim_start_matches("--endpoint=").to_string());
            }
            other if other.starts_with("--region=") => {
                region = other.trim_start_matches("--region=").to_string();
            }
            _ => {}
        }
        index += 1;
    }
    let bucket = bucket
        .or_else(|| std::env::var("CELLD_BUCKET").ok())
        .ok_or_else(|| anyhow!("lake requires --bucket or CELLD_BUCKET"))?;
    crate::fleet::bucket_client(&bucket, endpoint.as_deref(), &region)
}

fn print_lake_help() -> anyhow::Result<()> {
    crate::cli_output::Output::new(crate::cli_output::Format::Text).help(
        r#"celld lake — fleet analytics lake builder

USAGE:
  celld lake run [--daemon] --bucket [file://|s3://|gs://|az://]NAME[/PREFIX]
  celld lake compact --bucket [file://|s3://|gs://|az://]NAME[/PREFIX]
  celld lake status --bucket [file://|s3://|gs://|az://]NAME[/PREFIX]

ENVIRONMENT:
  CELLD_LAKE_INTERVAL_S       Seconds between daemon ticks (default: 30)
  CELLD_LAKE_WORKERS          Parallel cells per tick (default: 32)
  CELLD_LAKE_TARGET_PART_MB   Target fleet part size (default: 128)
  CELLD_LAKE_RETENTION_DAYS   Partition retention (default: 90; 0 = keep all)
  CELLD_ANALYTICS_ACK         `1` gates client ack on analytics HEAD proof
  CELLD_ANALYTICS_BATCH_TXIDS Coalesce outbox rows per delta (default: 64)
  CELLD_ANALYTICS_BATCH_MS    Max coalesce delay before flush (default: 250)
"#,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utc_partition_is_stable() {
        let partition = utc_date_partition(1_735_852_800_000);
        assert!(partition.starts_with("dt="));
    }
}
