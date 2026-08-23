# Fleet analytics lake

celld cells are the write path. The fleet lake is the read path. DuckDB
queries the lake, not thousands of per-cell prefixes.

This page specifies the lake builder: how per-cell analytics deltas become
query-optimized Parquet under `fleet/lake/`, and how operators run it.
Per-cell publish-on-ack (small deltas, epoch-fenced `HEAD`) is the ingest
contract; see the proposed ADR on lake publish in the RPO=0 ack path.
The lake builder is what makes **fast SQL across the whole fleet** possible.

## Problem

A fleet may hold thousands of cells. Each cell that opts into analytics
durability publishes under:

```
cells/<cell>/analytics/e<epoch>/HEAD
cells/<cell>/analytics/e<epoch>/delta-<min>-<max>.parquet
```

That layout is correct for **durability at ack time** and for **single-cell
inspection**. It is wrong for **interactive fleet-wide SQL**:

- DuckDB would need thousands of `HEAD` reads or prefix listings before
  planning a query.
- File counts explode (cells × deltas × retention).
- Cross-cell joins need **table-shaped** data, not cell-shaped shards.

The fleet lake is a **materialized, table-oriented** layer built from the
per-cell stream.

## Architecture

```
  Application (per cell)
       │
       │  SQL + _celld_analytics_outbox rows in one transaction
       ▼
  celld replicator (owner node)
       │  LTX durable + delta Parquet + HEAD  (ack gate)
       ▼
  cells/<cell>/analytics/e<epoch>/...       ◀── ingest (many small files)

       │
       │  lake builder (micro-batch default)
       ▼
  fleet/lake/<table>/dt=.../part-....parquet ◀── query (few large files)

       │
       ▼
  DuckDB  read_parquet / hive partitions / table manifests
```

| Layer | Owner | DuckDB reads it? | Freshness |
| --- | --- | --- | --- |
| Per-cell analytics | celld replicator | No (debug only) | RPO=0 at client ack |
| Fleet lake | `celld lake` builder | Yes | Checkpoint lag (default ~30 s) |

## Default mode: micro-batching

The first implementation runs a **periodic incremental merge** on a fixed
interval. This is the default because it balances freshness, cost, and
simplicity.

| Setting | Default | Meaning |
| --- | --- | --- |
| `CELLD_LAKE_INTERVAL_S` | `30` | Wake the builder every N seconds |
| `CELLD_LAKE_WORKERS` | `32` | Cells merged in parallel per tick |
| `CELLD_LAKE_TARGET_PART_MB` | `128` | Compact parts toward this size |
| `CELLD_LAKE_RETENTION_DAYS` | `90` | Drop fleet partitions older than N days (0 = keep all) |

On each tick:

1. Load `fleet/lake/_checkpoint.json`.
2. Discover cells (list `cells/*/own.json` or reuse fleet registry).
3. For each cell (bounded parallelism): read analytics `HEAD`; if
   `head.txid` is ahead of the checkpoint entry, fetch new deltas since
   the checkpoint.
4. Decode outbox rows; **route by `table_name`** into in-memory batches per
   logical table.
5. Append rows to the current hive partition for each table
   (`dt=<UTC-date>/`).
6. When a part file crosses `TARGET_PART_MB`, seal it and start a new part.
7. Update per-table `_manifest.json` (file list, row counts, byte size,
   max `txid` covered).
8. CAS-update `_checkpoint.json` (if-match on etag) with new high-water
   marks per cell.

A query at time *T* sees data that landed in the lake by the last
successful checkpoint before *T*. Typical lag: one interval plus merge
duration (often **30–60 s** under normal load).

Micro-batching keeps Class A operations predictable: one checkpoint write,
a bounded number of delta GETs per cell per tick, and append-only fleet
writes — not one fleet merge per client ack.

## Object layout

All paths are under the fleet bucket prefix (same as `CELLD_BUCKET`).

### Ingest (per cell, written by celld)

```
cells/<cell>/analytics/e<epoch>/HEAD
cells/<cell>/analytics/e<epoch>/delta-<min_txid>-<max_txid>.parquet
cells/<cell>/analytics/e<epoch>/base-<txid>.sqlite   # rare compaction
```

`HEAD` wire format (JSON):

```json
{
  "epoch": 7,
  "txid": 1050,
  "deltas": ["delta-1001-1050.parquet"],
  "base": null
}
```

### Fleet lake (written by lake builder)

```
fleet/lake/
  _checkpoint.json
  _builder.json              # last run metadata (optional, for diagnose)
  <table>/
    _manifest.json
    dt=2026-08-22/
      part-000001.parquet
      part-000002.parquet
```

**Checkpoint** (`_checkpoint.json`):

```json
{
  "version": 1,
  "cells": {
    "room-abc": { "epoch": 7, "txid": 1050 },
    "user-42": { "epoch": 3, "txid": 88 }
  },
  "updated_ms": 1730000000000
}
```

**Table manifest** (`<table>/_manifest.json`):

```json
{
  "version": 1,
  "table": "bookings",
  "files": [
    {
      "path": "fleet/lake/bookings/dt=2026-08-22/part-000001.parquet",
      "rows": 120000,
      "bytes": 134217728,
      "min_txid": 1,
      "max_txid": 50000
    }
  ],
  "partitions": ["dt=2026-08-22"]
}
```

Every merged row carries provenance columns appended by the builder:

| Column | Type | Meaning |
| --- | --- | --- |
| `_celld_cell` | string | Source cell id |
| `_celld_epoch` | int64 | Epoch at merge time |
| `_celld_txid` | int64 | Source transaction id |
| `_celld_merged_ms` | int64 | Builder timestamp |

Application payload columns come from the outbox `payload` (JSON or typed
Parquet written by the replicator). The builder does not infer schema from
arbitrary SQL; apps declare tables through outbox `table_name` and a
stable payload shape per table.

### Parquet schema (v1)

Cell deltas use a fixed envelope schema (replicator):

| Column | Type |
| --- | --- |
| `outbox_id` | int64 |
| `txid` | int64 |
| `table_name` | string |
| `op` | string |
| `row_key` | string (nullable) |
| `payload` | string (nullable, JSON UTF-8) |

The lake builder **denormalizes** into per-table files. For v1, payload
JSON is parsed and flattened when keys are consistent; otherwise payload
is stored as a JSON column `payload` on the fleet table until a registered
schema is added.

## DuckDB usage

Point DuckDB at manifests and partitions, not at `cells/*/analytics/`.

```sql
-- Latest partition for one table (example)
SELECT *
FROM read_parquet(
  's3://bucket/fleet/lake/bookings/dt=2026-08-22/*.parquet',
  hive_partitioning := true
)
WHERE amount > 100;

-- Cross-table join on fleet tables
SELECT b.id, b.amount, u.plan
FROM read_parquet('s3://bucket/fleet/lake/bookings/dt=2026-08-22/*.parquet') b
JOIN read_parquet('s3://bucket/fleet/lake/users/snapshot.parquet') u
  ON b.user_id = u.id;
```

For planning stability at scale, prefer the manifest file list over a
bucket-wide glob:

```sql
-- Application supplies paths from <table>/_manifest.json
SELECT * FROM read_parquet([$manifest_paths]);
```

A small query service (or `celld lake sql`) can load manifests, register
views, and expose HTTP SQL. That service is not required for v1; operators
can run DuckDB CLI against the bucket.

## CLI: `celld lake`

Subcommands (v1):

| Command | Purpose |
| --- | --- |
| `celld lake run` | Run one merge tick (also what a supervisor loops) |
| `celld lake run --daemon` | Loop every `CELLD_LAKE_INTERVAL_S` |
| `celld lake compact` | Merge small parts toward `TARGET_PART_MB` without ingesting |
| `celld lake status` | Print checkpoint lag, per-table file counts, last run |
| `celld lake sql 'SELECT ...'` | Optional: embedded DuckDB one-shot (later) |

`celld lake run` uses the same bucket credentials as `celld diagnose`.
It does not acquire cell ownership and does not serve Worker traffic.
One builder per fleet prefix is sufficient; runners coordinate through
checkpoint CAS.

## Relationship to celld write path

### Outbox table (cell DB)

Created by celld in `storage.rs` (reserved `_celld_*` namespace):

```sql
CREATE TABLE _celld_analytics_outbox (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  table_name TEXT NOT NULL,
  op TEXT NOT NULL,
  row_key TEXT,
  payload BLOB,
  published INTEGER NOT NULL DEFAULT 0
);
```

Applications insert outbox rows in the **same transaction** as business
writes. The replicator drains unpublished rows, encodes Parquet, PUTs
deltas, advances `HEAD`, and only then releases the output gate when
analytics ack is enabled for that class.

### Opt-in

| Surface | Flag |
| --- | --- |
| Fleet env | `CELLD_ANALYTICS_ACK=1` |
| Deploy / wrangler | `"analytics": { "enabled": true }` on a DO class |

Cells without analytics enabled produce no `cells/.../analytics/` objects;
the lake builder skips them.

## Alternatives (future modes)

Micro-batching is the default. The same object layout supports other
freshness/cost trade-offs without changing the DuckDB read contract.

### Continuous merge (lower lag, higher cost)

- Trigger a fleet append on every cell `HEAD` advance (webhook, bucket
  notification, or builder watching a queue).
- **Lag:** roughly one merge duration (seconds).
- **Cost:** more frequent small fleet parts; compaction must run often.
- **When:** billing dashboards that must be &lt;10 s behind ack.

### Scheduled batch (lowest cost, highest lag)

- Run `celld lake run` on a cron (e.g. every 15 minutes or hourly).
- **Lag:** one schedule period.
- **When:** nightly reporting, finance close, archival analytics.

### Streaming bus (decoupled ingest)

- Cell deltas are also published to Kafka / Pub/Sub / Redpanda; a
  separate consumer writes `fleet/lake/`.
- **Lag:** consumer offset behind.
- **When:** existing event pipeline, multi-subscriber fan-out, or cross-
  region replication. celld bucket remains source of truth; the bus is
  optional acceleration.

### Iceberg / Delta / Hudi catalog

- Fleet parts stay Parquet; manifest evolves into table snapshots with
  delete vectors and time travel.
- **When:** large data teams, ACID multi-writer lake, fine-grained
  retention. celld continues to PUT bytes; catalog logic stays outside
  the binary (or a thin `celld lake register` helper).

### Regional lakes

- Partition `fleet/lake/` by `region=<r>/` when cells are geo-placed.
- DuckDB queries one region or unions manifests explicitly.
- **When:** data residency or query locality dominates.

### On-node hot cache

- Builder or query service keeps the latest partition on NVMe.
- **When:** sub-second repeated queries on a stable dashboard slice.

None of these replace per-cell ack-time deltas. They only change how fast
ingest becomes fleet-visible.

## Compaction

Two compaction layers:

1. **Cell-side (rare):** `base-<txid>.sqlite` plus retained deltas —
   reduces delta count for the builder. Controlled by
   `CELLD_ANALYTICS_BASE_EVERY`.
2. **Fleet-side (regular):** `celld lake compact` merges
   `part-00000N.parquet` files within a partition until they approach
   `TARGET_PART_MB`, then updates `_manifest.json`.

Fleet compaction is independent of merge ticks and can run less often
(e.g. nightly).

## Failure and correctness

| Scenario | Behavior |
| --- | --- |
| Builder crashes mid-tick | Checkpoint not advanced; next run replays from last CAS |
| Duplicate delta read | Merge is idempotent on `(cell, txid, outbox id)` |
| Cell takeover (epoch bump) | Builder follows `HEAD.epoch`; old epoch prefixes ignored |
| Stale checkpoint vs HEAD | Builder only advances when delta objects exist |
| Two builders | Checkpoint CAS serializes; loser retries |

The lake is **eventually consistent** with respect to acked writes. It is
**not** a second durability boundary: restore still uses LTX only.

## Implementation phases

| Phase | Deliverable |
| --- | --- |
| 1 | Per-cell outbox + replicator delta Parquet + `HEAD` (ack gate) |
| 2 | `fleet/lake/_checkpoint.json`, builder tick, single-table merge |
| 3 | Multi-table routing, hive `dt=` partitions, `_manifest.json` |
| 4 | `celld lake run`, `status`, `compact`; `docs` + diagnose fields |
| 5 | Fleet compaction job, schema registry for typed columns |
| 6 | Optional `celld lake sql`, continuous mode, catalog adapters |

## Non-goals

- DuckDB reading live SQLite or LTX on the writer
- Interactive fleet SQL directly on `cells/*/analytics/**`
- Using fleet lake objects for cell restore
- Strong exactly-once visibility at the same instant as client ack (fleet
  lag is explicit and separate)

## References

- [Ownership and fencing](fencing.md) — epoch prefix, ack-after-bucket
- [Testing](testing.md) — durability and kill-test expectations
- Proposed ADR: lake publish on the RPO=0 ack path (per-cell ingest)
- `crates/celld/replication.rs` — `sqlite_snapshot`
- `crates/celld/telemetry.rs` — Parquet column writer pattern
