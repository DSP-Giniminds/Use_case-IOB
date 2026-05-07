#!/usr/bin/env bash
set -euo pipefail

#############################################
# IOB Aggregate Table Builder — Replicated Delta Mode
# (Updated delta logic: considers _deleted in per-event delta calculation)
#############################################

DB="IOB1"

USER="admin"
PASSWORD="Sruj@n13"

# Sources
SRC_RAW='IOB1."TBNK_ACCNTS"'
SRC_BRANCH='IOB1."BRMAST"'
SRC_REGION='IOB1."ROMAST"'
SRC_CDC_KAFKA='IOB1."TBNK_ACCNTS_KAFKA"'
SRC_CDC_BRMAST='IOB1."BRMAST_KAFKA"'
SRC_CDC_ROMAST='IOB1."ROMAST_KAFKA"'

# Dedup helper tables
DEDUP_BRANCH="${DB}.branch_master_dedup"
DEDUP_REGION="${DB}.region_master_dedup"

# Aggregate tables
TBL_REGION="${DB}.region_daily_accounts"
TBL_BRANCH="${DB}.branch_daily_accounts"
TBL_SCHM="${DB}.schmcode_trend"

# Materialized Views
MV_MAST_BRANCH="${DB}.mv_branch_master_dedup"
MV_MAST_REGION="${DB}.mv_region_master_dedup"
MV_REGION="${DB}.mv_region_daily_accounts_delta"
MV_BRANCH="${DB}.mv_branch_daily_accounts_delta"
MV_SCHM="${DB}.mv_schmcode_trend_delta"

clickhouse_exec() {
  clickhouse-client --host "$CLICKHOUSE_HOST" --port "$CLICKHOUSE_PORT" --user="$USER" --password="$PASSWORD" --multiquery --query="$1"
}

echo "=========================================="
echo " IOB Aggregate Table Builder — Replicated Delta Mode"
echo " Database     : ${DB}"
echo " Raw Snapshot : ${SRC_RAW}"
echo " CDC Kafka    : ${SRC_CDC_KAFKA}"
echo " Branch Master: ${SRC_BRANCH}"
echo " Region Master: ${SRC_REGION}"
echo "=========================================="

# 1️⃣ Drop old objects
echo "🔹 Dropping old views and tables..."
clickhouse_exec "
DROP VIEW IF EXISTS ${MV_REGION} ON CLUSTER default SYNC;
DROP VIEW IF EXISTS ${MV_BRANCH} ON CLUSTER default SYNC;
DROP VIEW IF EXISTS ${MV_SCHM} ON CLUSTER default SYNC;

DROP TABLE IF EXISTS ${TBL_REGION} ON CLUSTER default SYNC;
DROP TABLE IF EXISTS ${TBL_BRANCH} ON CLUSTER default SYNC;
DROP TABLE IF EXISTS ${TBL_SCHM} ON CLUSTER default SYNC;

DROP TABLE IF EXISTS ${DEDUP_BRANCH} ON CLUSTER default SYNC;
DROP TABLE IF EXISTS ${DEDUP_REGION} ON CLUSTER default SYNC;
"

# 2️⃣ Deduplicate master tables — replicated
echo "🔹 Creating replicated deduped master tables..."
clickhouse_exec "
CREATE TABLE ${DEDUP_BRANCH} ON default
(
    BRANCH_CODE String,
    BRANCH_NAME String,
    RO_CODE String,
    updated_at DateTime DEFAULT now(),
    _deleted UInt8 DEFAULT 0 
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/IOB1/branch_master_dedup','{replica}', updated_at)
ORDER BY BRANCH_CODE
SETTINGS index_granularity = 8192;

INSERT INTO ${DEDUP_BRANCH}
SELECT
    BRANCH_CODE,
    any(BRANCH_NAME) AS BRANCH_NAME,
    any(RO_CODE) AS RO_CODE,
    now() AS updated_at,
    0 AS _deleted
FROM ${SRC_BRANCH} FINAL
GROUP BY BRANCH_CODE;

CREATE TABLE ${DEDUP_REGION} ON CLUSTER default
(
    RO_CODE String,
    REGION_NAME String,
    updated_at DateTime DEFAULT now(),
    _deleted UInt8 DEFAULT 0
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/IOB1/region_master_dedup','{replica}', updated_at)
ORDER BY RO_CODE
SETTINGS index_granularity = 8192;

INSERT INTO ${DEDUP_REGION}
SELECT
    RO_CODE,
    any(REGION_NAME) AS REGION_NAME,
    now() AS updated_at,
    0 AS _deleted
FROM ${SRC_REGION} FINAL
GROUP BY RO_CODE;



/* =========================================================
    BRANCH MATERIALIZED VIEW
   ========================================================= */

CREATE MATERIALIZED VIEW IF NOT EXISTS ${MV_MAST_BRANCH} ON CLUSTER default
TO ${DEDUP_BRANCH} 
AS
SELECT
    if(op = 'd',
        JSONExtractString(toString(before), 'BRANCH_CODE'),
        JSONExtractString(toString(after), 'BRANCH_CODE')
    ) AS BRANCH_CODE,

    if(op = 'd',
        JSONExtractString(toString(before), 'BRANCH_NAME'),
        JSONExtractString(toString(after), 'BRANCH_NAME')
    ) AS BRANCH_NAME,

    if(op = 'd',
        JSONExtractString(toString(before), 'RO_CODE'),
        JSONExtractString(toString(after), 'RO_CODE')
    ) AS RO_CODE,

    if(ts_ms > 0, toDateTime(intDiv(ts_ms, 1000)), now()) AS updated_at,

    if(op = 'd', 1, 0) AS _deleted

FROM ${SRC_CDC_BRMAST};


/* =========================================================
    REGION MATERIALIZED VIEW
   ========================================================= */

CREATE MATERIALIZED VIEW IF NOT EXISTS ${MV_MAST_REGION} ON CLUSTER default
TO ${DEDUP_REGION}
AS
SELECT
    if(op = 'd',
        JSONExtractString(toString(before), 'RO_CODE'),
        JSONExtractString(toString(after), 'RO_CODE')
    ) AS RO_CODE,

    if(op = 'd',
        JSONExtractString(toString(before), 'REGION_NAME'),
        JSONExtractString(toString(after), 'REGION_NAME')
    ) AS REGION_NAME,

    if(ts_ms > 0, toDateTime(intDiv(ts_ms, 1000)), now()) AS updated_at,

    if(op = 'd', 1, 0) AS _deleted

FROM ${SRC_CDC_ROMAST};
"

echo "✅ Replicated dedup master tables ready."

# 3️⃣ Create replicated aggregate target tables (delta-enabled)
echo "🔹 Creating replicated aggregate target tables..."
clickhouse_exec "
CREATE TABLE ${TBL_REGION} ON CLUSTER default
(
    REGION_NAME String,
    TXN_DT Date,
    AccountsDelta Int64
)
ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{shard}/IOB1/region_daily_accounts','{replica}')
PARTITION BY toYYYYMM(TXN_DT)
ORDER BY (REGION_NAME, TXN_DT);

CREATE TABLE ${TBL_BRANCH} ON CLUSTER default
(
    BRANCH_NAME String,
    REGION_NAME String,
    TXN_DT Date,
    AccountsDelta Int64
)
ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{shard}/IOB1/branch_daily_accounts','{replica}')
PARTITION BY toYYYYMM(TXN_DT)
ORDER BY (REGION_NAME, BRANCH_NAME, TXN_DT);

CREATE TABLE ${TBL_SCHM} ON CLUSTER default
(
    BRANCH_CODE String,
    REGION_NAME String,
    SCHMCODE String,
    TXN_DT Date,
    AccountsDelta Int64
)
ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{shard}/IOB1/schmcode_trend','{replica}')
PARTITION BY toYYYYMM(TXN_DT)
ORDER BY (REGION_NAME, BRANCH_CODE, SCHMCODE, TXN_DT);
"

echo "✅ Replicated aggregate tables created."

# 4️⃣ Backfill snapshot (delta=+1)
echo "🔹 Backfilling replicated aggregates..."
clickhouse_exec "
-- Region-level backfill
INSERT INTO ${TBL_REGION}
SELECT
    coalesce(r.REGION_NAME, 'UNKNOWN') AS REGION_NAME,
    toDate(parseDateTimeBestEffortOrNull(t.TXN_DT)) AS TXN_DT,
    toInt64(count()) AS AccountsDelta
FROM ${SRC_RAW} AS t
LEFT JOIN ${DEDUP_BRANCH} AS b FINAL ON trim(t.SOLID) = trim(b.BRANCH_CODE)
LEFT JOIN ${DEDUP_REGION} AS r FINAL ON b.RO_CODE = r.RO_CODE
WHERE lower(trim(t.STATUS)) IN ('success','completed','ok')
  AND (t._deleted = 0 OR t._deleted IS NULL)
GROUP BY r.REGION_NAME, TXN_DT;

-- Branch-level backfill
INSERT INTO ${TBL_BRANCH}
SELECT
    coalesce(b.BRANCH_NAME, 'UNKNOWN') AS BRANCH_NAME,
    coalesce(r.REGION_NAME, 'UNKNOWN') AS REGION_NAME,
    toDate(parseDateTimeBestEffortOrNull(t.TXN_DT)) AS TXN_DT,
    toInt64(count()) AS AccountsDelta
FROM ${SRC_RAW} AS t
LEFT JOIN ${DEDUP_BRANCH} AS b FINAL ON trim(t.SOLID) = trim(b.BRANCH_CODE)
LEFT JOIN ${DEDUP_REGION} AS r FINAL ON b.RO_CODE = r.RO_CODE
WHERE lower(trim(t.STATUS)) IN ('success','completed','ok')
  AND (t._deleted = 0 OR t._deleted IS NULL)
GROUP BY b.BRANCH_NAME, r.REGION_NAME, TXN_DT;

-- Schmcode-level backfill
INSERT INTO ${TBL_SCHM}
SELECT
    trim(t.SOLID) AS BRANCH_CODE,
    coalesce(r.REGION_NAME, 'UNKNOWN') AS REGION_NAME,
    trim(t.SCHMCODE) AS SCHMCODE,
    toDate(parseDateTimeBestEffortOrNull(t.TXN_DT)) AS TXN_DT,
    toInt64(count()) AS AccountsDelta
FROM ${SRC_RAW} AS t
LEFT JOIN ${DEDUP_BRANCH} AS b FINAL ON trim(t.SOLID) = trim(b.BRANCH_CODE)
LEFT JOIN ${DEDUP_REGION} AS r FINAL ON b.RO_CODE = r.RO_CODE
WHERE lower(trim(t.STATUS)) IN ('success','completed','ok')
  AND (t._deleted = 0 OR t._deleted IS NULL)
GROUP BY BRANCH_CODE, REGION_NAME, SCHMCODE, TXN_DT;
"

echo "✅ Replicated backfill completed."

# 5️⃣ Materialized Views for CDC deltas (updated delta logic includes _deleted)
echo "🔹 Creating materialized views for live deltas..."
clickhouse_exec "
-- Helper: delta expression used in all MVs:
-- (
--   (
--     (after_status_is_success) AND coalesce(after._deleted,0)=0
--   )::Int8
--   -
--   (
--     (before_status_is_success) AND coalesce(before._deleted,0)=0
--   )::Int8
-- )

-- Region MV
CREATE MATERIALIZED VIEW IF NOT EXISTS ${MV_REGION} ON CLUSTER default TO ${TBL_REGION} AS
SELECT
    coalesce(r.REGION_NAME, 'UNKNOWN') AS REGION_NAME,
    toDate(parseDateTimeBestEffortOrNull(
        if(length(JSONExtractString(toString(after), 'TXN_DT'))>0,
           JSONExtractString(toString(after), 'TXN_DT'),
           JSONExtractString(toString(before), 'TXN_DT')
        )
    )) AS TXN_DT,
    (
      (
        (lower(trim(coalesce(JSONExtractString(toString(after),'STATUS'),''))) IN ('success','completed','ok'))
        AND coalesce(JSONExtractUInt(toString(after), '_deleted'), 0) = 0
      )::Int8
      -
      (
        (lower(trim(coalesce(JSONExtractString(toString(before),'STATUS'),''))) IN ('success','completed','ok'))
        AND coalesce(JSONExtractUInt(toString(before), '_deleted'), 0) = 0
      )::Int8
    ) AS AccountsDelta
FROM ${SRC_CDC_KAFKA} AS k
LEFT JOIN ${DEDUP_BRANCH} AS b FINAL ON trim(
     if(length(JSONExtractString(toString(after),'SOLID'))>0,
        JSONExtractString(toString(after),'SOLID'),
        JSONExtractString(toString(before),'SOLID')
     )
) = trim(b.BRANCH_CODE)
LEFT JOIN ${DEDUP_REGION} AS r FINAL ON b.RO_CODE = r.RO_CODE
WHERE
    (
      (
        (lower(trim(coalesce(JSONExtractString(toString(after),'STATUS'),''))) IN ('success','completed','ok'))
        AND coalesce(JSONExtractUInt(toString(after), '_deleted'), 0) = 0
      )::Int8
      -
      (
        (lower(trim(coalesce(JSONExtractString(toString(before),'STATUS'),''))) IN ('success','completed','ok'))
        AND coalesce(JSONExtractUInt(toString(before), '_deleted'), 0) = 0
      )::Int8
    ) != 0
;

-- Branch MV
CREATE MATERIALIZED VIEW IF NOT EXISTS ${MV_BRANCH} ON CLUSTER default TO ${TBL_BRANCH} AS
SELECT
    coalesce(b.BRANCH_NAME, 'UNKNOWN') AS BRANCH_NAME,
    coalesce(r.REGION_NAME, 'UNKNOWN') AS REGION_NAME,
    toDate(parseDateTimeBestEffortOrNull(
        if(length(JSONExtractString(toString(after), 'TXN_DT'))>0,
           JSONExtractString(toString(after), 'TXN_DT'),
           JSONExtractString(toString(before), 'TXN_DT')
        )
    )) AS TXN_DT,
    (
      (
        (lower(trim(coalesce(JSONExtractString(toString(after),'STATUS'),''))) IN ('success','completed','ok'))
        AND coalesce(JSONExtractUInt(toString(after), '_deleted'), 0) = 0
      )::Int8
      -
      (
        (lower(trim(coalesce(JSONExtractString(toString(before),'STATUS'),''))) IN ('success','completed','ok'))
        AND coalesce(JSONExtractUInt(toString(before), '_deleted'), 0) = 0
      )::Int8
    ) AS AccountsDelta
FROM ${SRC_CDC_KAFKA} AS k
LEFT JOIN ${DEDUP_BRANCH} AS b FINAL ON trim(
     if(length(JSONExtractString(toString(after),'SOLID'))>0,
        JSONExtractString(toString(after),'SOLID'),
        JSONExtractString(toString(before),'SOLID')
     )
) = trim(b.BRANCH_CODE)
LEFT JOIN ${DEDUP_REGION} AS r FINAL ON b.RO_CODE = r.RO_CODE
WHERE
    (
      (
        (lower(trim(coalesce(JSONExtractString(toString(after),'STATUS'),''))) IN ('success','completed','ok'))
        AND coalesce(JSONExtractUInt(toString(after), '_deleted'), 0) = 0
      )::Int8
      -
      (
        (lower(trim(coalesce(JSONExtractString(toString(before),'STATUS'),''))) IN ('success','completed','ok'))
        AND coalesce(JSONExtractUInt(toString(before), '_deleted'), 0) = 0
      )::Int8
    ) != 0
;

-- Schmcode MV (with REGION_NAME)
CREATE MATERIALIZED VIEW IF NOT EXISTS ${MV_SCHM} ON CLUSTER default TO ${TBL_SCHM} AS
SELECT
    trim(
      if(length(JSONExtractString(toString(after),'SOLID'))>0,
         JSONExtractString(toString(after),'SOLID'),
         JSONExtractString(toString(before),'SOLID')
      )
    ) AS BRANCH_CODE,
    coalesce(r.REGION_NAME, 'UNKNOWN') AS REGION_NAME,
    trim(
      if(length(JSONExtractString(toString(after),'SCHMCODE'))>0,
         JSONExtractString(toString(after),'SCHMCODE'),
         JSONExtractString(toString(before),'SCHMCODE')
      )
    ) AS SCHMCODE,
    toDate(parseDateTimeBestEffortOrNull(
      if(length(JSONExtractString(toString(after),'TXN_DT'))>0,
         JSONExtractString(toString(after),'TXN_DT'),
         JSONExtractString(toString(before),'TXN_DT')
      )
    )) AS TXN_DT,
    (
      (
        (lower(trim(coalesce(JSONExtractString(toString(after),'STATUS'),''))) IN ('success','completed','ok'))
        AND coalesce(JSONExtractUInt(toString(after), '_deleted'), 0) = 0
      )::Int8
      -
      (
        (lower(trim(coalesce(JSONExtractString(toString(before),'STATUS'),''))) IN ('success','completed','ok'))
        AND coalesce(JSONExtractUInt(toString(before), '_deleted'), 0) = 0
      )::Int8
    ) AS AccountsDelta
FROM ${SRC_CDC_KAFKA} AS k
LEFT JOIN ${DEDUP_BRANCH} AS b FINAL ON trim(
     if(length(JSONExtractString(toString(after),'SOLID'))>0,
        JSONExtractString(toString(after),'SOLID'),
        JSONExtractString(toString(before),'SOLID')
     )
) = trim(b.BRANCH_CODE)
LEFT JOIN ${DEDUP_REGION} AS r FINAL ON b.RO_CODE = r.RO_CODE
WHERE
    (
      (
        (lower(trim(coalesce(JSONExtractString(toString(after),'STATUS'),''))) IN ('success','completed','ok'))
        AND coalesce(JSONExtractUInt(toString(after), '_deleted'), 0) = 0
      )::Int8
      -
      (
        (lower(trim(coalesce(JSONExtractString(toString(before),'STATUS'),''))) IN ('success','completed','ok'))
        AND coalesce(JSONExtractUInt(toString(before), '_deleted'), 0) = 0
      )::Int8
    ) != 0
;
"

echo "✅ Replicated materialized views (delta) created."

# 6️⃣ Verification
echo "🔹 Verification Summary (replicated deltas applied to aggregates)"
clickhouse_exec "
SELECT 'region_total' AS name, count() AS rows, sum(AccountsDelta) AS net_accounts FROM ${TBL_REGION};
SELECT 'branch_total' AS name, count() AS rows, sum(AccountsDelta) AS net_accounts FROM ${TBL_BRANCH};
SELECT 'schm_total' AS name, count() AS rows, sum(AccountsDelta) AS net_accounts FROM ${TBL_SCHM};
"

echo "=========================================="
echo "✅ IOB Aggregate Build (replicated delta-enabled, REGION_NAME added) Completed"
echo "=========================================="

echo
echo "Notes and recommended run procedure:"
echo " - If materialized views were active during a previous backfill, you may have duplicate/extra deltas."
echo "   Recommended clean rebuild:"
echo "     1) DROP VIEW IF EXISTS <mv> SYNC  (for all three MVs)"
echo "     2) TRUNCATE TABLE <target_table> (region_daily_accounts, branch_daily_accounts, schmcode_trend)"
echo "     3) Run this script (it will recreate MVs and run backfill)"
echo
echo " - Replace '/clickhouse/tables/{shard}/...' and '{replica}' with your real ZooKeeper path and replica names."
echo " - If you want stronger ordering guarantees for late events, consider adding event timestamp and using ReplacingMergeTree on that timestamp to dedupe by latest event."
