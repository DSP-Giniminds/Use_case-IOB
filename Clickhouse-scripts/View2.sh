#!/usr/bin/env bash
set -euo pipefail

# region_branch_summary_delta_replica.sh
# Real-time CDC-aware Region → Branch summary (delta-based) — replicated
# Uses CDC Kafka envelope: CDC."TBNK_ACCNTS_KAFKA"
# Backfill snapshot -> delta=+1, and MVs emit (after_success - before_success).

CLICKHOUSE_HOST="192.168.80.130"
CLICKHOUSE_PORT="9000"
USER="admin"
PASSWORD="Sruj@n13"

DB="IOB1"
SRC_RAW='IOB1."TBNK_ACCNTS"'            # current-state ReplacingMergeTree table
SRC_CDC_KAFKA='IOB1."TBNK_ACCNTS_KAFKA"' # CDC Kafka engine table (Debezium envelope)
SRC_BRANCH='IOB1."BRMAST"'                   # branch master (CDC version)
SRC_REGION='IOB1."ROMAST"'                   # region master (CDC version)

DEDUP_BRANCH="${DB}.branch_master_dedup"
DEDUP_REGION="${DB}.region_master_dedup"

SUMMARY_TBL="${DB}.customer_region_branch_summary"
SUMMARY_MV="${DB}.mv_customer_region_branch_summary_delta"

clickhouse_exec() {
  clickhouse-client --multiquery --host="$CLICKHOUSE_HOST" --port="$CLICKHOUSE_PORT" --user="$USER" --password="$PASSWORD" --query="$1"
}

echo "=========================================="
echo " IOB Region-Branch Delta Summary Builder (replicated)"
echo " Database   : ${DB}"
echo " Snapshot   : ${SRC_RAW}"
echo " CDC Kafka  : ${SRC_CDC_KAFKA}"
echo " Branch Src : ${SRC_BRANCH}"
echo " Region Src : ${SRC_REGION}"
echo "=========================================="

# 1️⃣ Drop old summary MV/table
echo "🔹 Dropping old view/table (if exists)..."
clickhouse_exec "
DROP VIEW IF EXISTS ${SUMMARY_MV} SYNC;
DROP TABLE IF EXISTS ${SUMMARY_TBL} SYNC;
"


echo "✅ Replicated dedup masters are ready."

# 3️⃣ Create the ReplicatedSummingMergeTree summary table
echo "🔹 Creating replicated summary table ${SUMMARY_TBL}..."
clickhouse_exec "
-- NOTE: replace '/clickhouse/tables/{shard}/customer_region_branch_summary' and '{replica}' with actual values
CREATE TABLE ${SUMMARY_TBL} ON CLUSTER default
(
    REGION_NAME String,
    BRANCH_NAME String,
    AccountsDelta Int64
)
ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{shard}/IOB1/customer_region_branch_summary','{replica}')
ORDER BY (REGION_NAME, BRANCH_NAME);
"

echo "✅ Replicated summary table created."

# 4️⃣ Backfill snapshot -> +1 for each currently-successful row
echo "🔹 Backfilling summary from snapshot..."
clickhouse_exec "
INSERT INTO ${SUMMARY_TBL}
SELECT
    coalesce(r.REGION_NAME, 'UNKNOWN') AS REGION_NAME,
    coalesce(b.BRANCH_NAME, 'UNKNOWN') AS BRANCH_NAME,
    toInt64(count()) AS AccountsDelta
FROM ${SRC_RAW} AS t
LEFT JOIN ${DEDUP_BRANCH} AS b FINAL ON trim(t.SOLID) = trim(b.BRANCH_CODE)
LEFT JOIN ${DEDUP_REGION} AS r FINAL ON b.RO_CODE = r.RO_CODE
WHERE lower(trim(t.STATUS)) IN ('success','completed','ok')
  AND (t._deleted = 0 OR t._deleted IS NULL)
GROUP BY r.REGION_NAME, b.BRANCH_NAME;
"

echo "✅ Backfill completed."

# 5️⃣ Materialized View — emits deltas from CDC Kafka envelope
echo "🔹 Creating materialized view ${SUMMARY_MV}..."
clickhouse_exec "
CREATE MATERIALIZED VIEW ${SUMMARY_MV} ON CLUSTER default TO ${SUMMARY_TBL} AS
SELECT
    coalesce(r.REGION_NAME, 'UNKNOWN') AS REGION_NAME,
    coalesce(b.BRANCH_NAME, 'UNKNOWN') AS BRANCH_NAME,
    (
      (lower(trim(coalesce(JSONExtractString(toString(after),'STATUS'), ''))) IN ('success','completed','ok'))::Int8
      -
      (lower(trim(coalesce(JSONExtractString(toString(before),'STATUS'), ''))) IN ('success','completed','ok'))::Int8
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
      (lower(trim(coalesce(JSONExtractString(toString(after),'STATUS'), ''))) IN ('success','completed','ok'))::Int8
      -
      (lower(trim(coalesce(JSONExtractString(toString(before),'STATUS'), ''))) IN ('success','completed','ok'))::Int8
    ) != 0;
"

echo "✅ Materialized view created and streaming CDC events."

# 6️⃣ Verification queries
echo "🔹 Running verification queries..."
clickhouse_exec "
SELECT 'summary_total_rows' AS name, count() AS rows, sum(AccountsDelta) AS net_accounts FROM ${SUMMARY_TBL};
SELECT 'snapshot_success_total' AS name, count() AS rows FROM ${SRC_RAW} WHERE lower(trim(STATUS)) IN ('success','completed','ok') AND (_deleted = 0 OR _deleted IS NULL);
"

echo "=========================================="
echo "✅ Region-Branch Delta Summary Build (replicated) Completed Successfully"
echo "=========================================="

echo
echo "Notes:"
echo "- Replace '/clickhouse/tables/{shard}/...' and '{replica}' with real ZooKeeper paths and replica names."
echo "- If MVs were active during backfill, TRUNCATE ${SUMMARY_TBL} and re-run backfill to realign."
echo "- To expose a cluster-wide entrypoint, create a Distributed table (example below)."
echo
echo "Distributed example (run once, replace iob_cluster with your cluster name):"
echo "CREATE TABLE ${DB}.customer_region_branch_summary_dist AS ${DB}.customer_region_branch_summary"
echo "ENGINE = Distributed('iob_cluster', '${DB}', 'customer_region_branch_summary', rand());"
