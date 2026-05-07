#!/usr/bin/env bash
set -euo pipefail

#################################################################
# Dynamic Configurations
#################################################################
VERSION=${1:-20}      # default version is 20; override as ./script.sh 21
CLICKHOUSE_HOST="192.168.80.130"
CLICKHOUSE_PORT="9000"
CLICKHOUSE_DB="IOB1"
CLUSTER_NAME="default"
USER="admin"
PASSWORD="Suj@n13"

# Source CDC Kafka engine table (Debezium Topic in ClickHouse)
CDC_KAFKA_TABLE='IOB1."TBNK_CIF_KAFKA"'

# Source snapshot table (one-time backfill from Oracle)
SNAPSHOT_SOURCE='IOB1."TBNK_CIF"'

# Dedup master tables
BRANCH_DEDUP_TABLE="branch_master_dedup"
REGION_DEDUP_TABLE="region_master_dedup"

# Aggregated final table (replicated)
AGG_TABLE="customer_demographics${VERSION}"
MV_NAME="mv_customer_demographics${VERSION}"

# Unique identifier for uniqExact()
UNIQUE_ID="TXN_ID"

#################################################################
echo "=========================================================="
echo "🚀 Starting Fully Dynamic CDC Aggregation v${VERSION} (No Staging Table)"
echo "📌 Source Snapshot   : ${SNAPSHOT_SOURCE}"
echo "📌 Source CDC Kafka  : ${CDC_KAFKA_TABLE}"
echo "📌 Aggregated Table  : ${CLICKHOUSE_DB}.${AGG_TABLE}"
echo "📌 Materialized View : ${CLICKHOUSE_DB}.${MV_NAME}"
echo "📌 Branch Dedup      : ${CLICKHOUSE_DB}.${BRANCH_DEDUP_TABLE}"
echo "📌 Region Dedup      : ${CLICKHOUSE_DB}.${REGION_DEDUP_TABLE}"
echo "=========================================================="

clickhouse-client --host "$CLICKHOUSE_HOST" --port "$CLICKHOUSE_PORT" --user="$USER" --password="$PASSWORD" --multiquery <<EOSQL

-- 0️⃣ Cleanup old version (if exists)
DROP VIEW IF EXISTS ${CLICKHOUSE_DB}.${MV_NAME} ON CLUSTER ${CLUSTER_NAME} SYNC;
DROP TABLE IF EXISTS ${CLICKHOUSE_DB}.${AGG_TABLE} ON CLUSTER ${CLUSTER_NAME} SYNC;

-- 1️⃣ Create NEW final replicated aggregation table
CREATE TABLE ${CLICKHOUSE_DB}.${AGG_TABLE} ON CLUSTER ${CLUSTER_NAME}
(
    REGION_NAME       LowCardinality(String),
    BRCODE            LowCardinality(String),
    INCOMESOURCE      LowCardinality(String),
    EMPLOYMENTSTATUS  LowCardinality(String),
    EDUQUALIFICATION  LowCardinality(String),

    -- ✅ Make income metrics Nullable to avoid NULL → non-NULL cast errors
    TOTALINCOME_CLEAN Nullable(Float64),
    TOTAL_CUSTOMERS   UInt64,
    AVG_INCOME        Nullable(Float64),
    MEDIAN_INCOME     Nullable(Float64),
    STDDEV_INCOME     Nullable(Float64),

    TXN_DT            String,
    updated_at        DateTime DEFAULT now()
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/IOB1/${AGG_TABLE}', '{replica}', updated_at
)
PARTITION BY substring(TXN_DT, 1, 6)
ORDER BY (TXN_DT, REGION_NAME, BRCODE, INCOMESOURCE, EMPLOYMENTSTATUS, EDUQUALIFICATION);

-- 2️⃣ One-time Backfill (from Oracle snapshot / base table)
INSERT INTO ${CLICKHOUSE_DB}.${AGG_TABLE}
SELECT
    coalesce(r.REGION_NAME, 'UNKNOWN') AS REGION_NAME,
    trimBoth(c.BRCODE) AS BRCODE,
    multiIf(
        lower(trimBoth(c.INCOMESOURCE)) IN ('saving','savng'), 'SAVING',
        (c.INCOMESOURCE = '' OR c.INCOMESOURCE IS NULL), 'NOT AVAILABLE',
        trimBoth(c.INCOMESOURCE)
    ) AS INCOMESOURCE,
    multiIf(
        lower(trimBoth(c.EMPLOYMENTSTATUS)) LIKE '%self%', 'SELF-EMPLOYED',
        (c.EMPLOYMENTSTATUS = '' OR c.EMPLOYMENTSTATUS IS NULL), 'NOT AVAILABLE',
        trimBoth(c.EMPLOYMENTSTATUS)
    ) AS EMPLOYMENTSTATUS,
    multiIf(
        (c.EDUQUALIFICATION = '' OR c.EDUQUALIFICATION IS NULL), 'NOT AVAILABLE',
        trimBoth(c.EDUQUALIFICATION)
    ) AS EDUQUALIFICATION,
    sum(toFloat64OrZero(nullIf(trimBoth(c.TOTALINCOME), ''))) AS TOTALINCOME_CLEAN,
    uniqExact(trimBoth(c.${UNIQUE_ID})) AS TOTAL_CUSTOMERS,
    round(avg(toFloat64OrZero(nullIf(trimBoth(c.TOTALINCOME), ''))), 2) AS AVG_INCOME,
    quantileExact(0.5)(
        toFloat64OrZero(nullIf(trimBoth(c.TOTALINCOME), ''))
    ) AS MEDIAN_INCOME,
    round(stddevPop(
        toFloat64OrZero(nullIf(trimBoth(c.TOTALINCOME), ''))
    ), 2) AS STDDEV_INCOME,
    toString(c.TXN_DT) AS TXN_DT,
    now() AS updated_at
FROM ${SNAPSHOT_SOURCE} AS c
LEFT JOIN ${CLICKHOUSE_DB}.${BRANCH_DEDUP_TABLE} b FINAL
    ON lower(trimBoth(c.BRCODE)) = lower(trimBoth(b.BRANCH_CODE))
LEFT JOIN ${CLICKHOUSE_DB}.${REGION_DEDUP_TABLE} r FINAL
    ON b.RO_CODE = r.RO_CODE
WHERE lower(c.STATUS) = 'success'
  AND (c._deleted = 0 OR c._deleted IS NULL)
GROUP BY REGION_NAME, BRCODE, INCOMESOURCE, EMPLOYMENTSTATUS, EDUQUALIFICATION, TXN_DT;

-- 3️⃣ Live CDC MV — directly from Kafka to new final table
CREATE MATERIALIZED VIEW ${CLICKHOUSE_DB}.${MV_NAME}
ON CLUSTER ${CLUSTER_NAME}
TO ${CLICKHOUSE_DB}.${AGG_TABLE}
AS
SELECT
    coalesce(r.REGION_NAME, 'UNKNOWN') AS REGION_NAME,
    trimBoth(JSONExtractString(toString(after),'BRCODE')) AS BRCODE,
    multiIf(
        lower(trimBoth(JSONExtractString(toString(after),'INCOMESOURCE'))) IN ('saving','savng'), 'SAVING',
        JSONExtractString(toString(after),'INCOMESOURCE') = '' OR JSONExtractString(toString(after),'INCOMESOURCE') IS NULL, 'NOT AVAILABLE',
        trimBoth(JSONExtractString(toString(after),'INCOMESOURCE'))
    ) AS INCOMESOURCE,
    multiIf(
        lower(trimBoth(JSONExtractString(toString(after),'EMPLOYMENTSTATUS'))) LIKE '%self%', 'SELF-EMPLOYED',
        JSONExtractString(toString(after),'EMPLOYMENTSTATUS') = '' OR JSONExtractString(toString(after),'EMPLOYMENTSTATUS') IS NULL, 'NOT AVAILABLE',
        trimBoth(JSONExtractString(toString(after),'EMPLOYMENTSTATUS'))
    ) AS EMPLOYMENTSTATUS,
    multiIf(
        JSONExtractString(toString(after),'EDUQUALIFICATION') = '' OR JSONExtractString(toString(after),'EDUQUALIFICATION') IS NULL, 'NOT AVAILABLE',
        trimBoth(JSONExtractString(toString(after),'EDUQUALIFICATION'))
    ) AS EDUQUALIFICATION,

    -- these now safely produce Nullable(Float64) which matches table schema
    sum(
        toFloat64OrZero(nullIf(trimBoth(JSONExtractString(toString(after),'TOTALINCOME')), ''))
    ) AS TOTALINCOME_CLEAN,
    uniqExact(
        trimBoth(JSONExtractString(toString(after),'${UNIQUE_ID}'))
    ) AS TOTAL_CUSTOMERS,
    round(
        avg(toFloat64OrZero(nullIf(trimBoth(JSONExtractString(toString(after),'TOTALINCOME')), ''))),
        2
    ) AS AVG_INCOME,
    quantileExact(0.5)(
        toFloat64OrZero(nullIf(trimBoth(JSONExtractString(toString(after),'TOTALINCOME')), ''))
    ) AS MEDIAN_INCOME,
    round(
        stddevPop(toFloat64OrZero(nullIf(trimBoth(JSONExtractString(toString(after),'TOTALINCOME')), ''))),
        2
    ) AS STDDEV_INCOME,

    toString(JSONExtractString(toString(after),'TXN_DT')) AS TXN_DT,
    now() AS updated_at
FROM ${CDC_KAFKA_TABLE} AS k
LEFT JOIN ${CLICKHOUSE_DB}.${BRANCH_DEDUP_TABLE} b FINAL
    ON trimBoth(JSONExtractString(toString(after),'BRCODE')) = trimBoth(b.BRANCH_CODE)
LEFT JOIN ${CLICKHOUSE_DB}.${REGION_DEDUP_TABLE} r FINAL
    ON b.RO_CODE = r.RO_CODE
WHERE
    lower(trimBoth(JSONExtractString(toString(after),'STATUS'))) = 'success'
    AND toUInt8(JSONExtractInt(toString(after),'_deleted')) = 0
GROUP BY REGION_NAME, BRCODE, INCOMESOURCE, EMPLOYMENTSTATUS, EDUQUALIFICATION, TXN_DT;

EOSQL

echo "=========================================================="
echo "🎯 Completed Fully Dynamic CDC Aggregation v${VERSION} (No Staging, New Table)"
echo "=========================================================="
