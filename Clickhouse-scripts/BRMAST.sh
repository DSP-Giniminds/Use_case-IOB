#!/bin/bash
# ============================================================
# Script: br.sh
# Purpose: CDC ingestion for BRMAST
# Handles insert, update, delete in real-time
# ============================================================

CLICKHOUSE_HOST="192.168.80.130"
CLICKHOUSE_PORT="9000"
USER="admin"
PASSWORD="Sruj@n13"

clickhouse-client --host "$CLICKHOUSE_HOST" --port "$CLICKHOUSE_PORT" --user="$USER" --password="$PASSWORD" --multiquery <<'EOSQL'

-- 1️⃣ Target Table (with CDC-safe Replacing engine)
CREATE TABLE IF NOT EXISTS IOB1."BRMAST" ON CLUSTER default
(
    BRANCH_CODE String,
    BRANCH_NAME String,
    RO_CODE String,
    updated_at DateTime DEFAULT now(),
    _deleted UInt8 DEFAULT 0
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/{database}/{table}', 
    '{replica}', 
    updated_at
)
ORDER BY BRANCH_CODE;

-- 2️⃣ Kafka Engine Table (Debezium CDC message structure)
CREATE TABLE IF NOT EXISTS IOB1."BRMAST_KAFKA" ON CLUSTER default
(
    before JSON,
    after JSON,
    op String,
    ts_ms UInt64
)
ENGINE = Kafka
SETTINGS kafka_broker_list = '192.168.80.130:9094,192.168.80.131:9094',
         kafka_topic_list = 'test296.C__IOB1.BRMAST',
         kafka_group_name = 'BRMAST_011',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 2;

-- 3️⃣ Materialized View: Transform CDC events into the target table
CREATE MATERIALIZED VIEW IF NOT EXISTS IOB1."BRMAST_MV" ON CLUSTER default
TO IOB1."BRMAST"
AS
SELECT
    -- Choose before or after based on operation type
    if(op = 'd', JSONExtractString(toString(before), 'BRANCH_CODE'), JSONExtractString(toString(after), 'BRANCH_CODE')) AS BRANCH_CODE,
    if(op = 'd', JSONExtractString(toString(before), 'BRANCH_NAME'), JSONExtractString(toString(after), 'BRANCH_NAME')) AS BRANCH_NAME,
    if(op = 'd', JSONExtractString(toString(before), 'RO_CODE'), JSONExtractString(toString(after), 'RO_CODE')) AS RO_CODE,
    if(ts_ms > 0, toDateTime(intDiv(ts_ms, 1000)), now()) AS updated_at,
    if(op = 'd', 1, 0) AS _deleted
FROM IOB1."BRMAST_KAFKA";

EOSQL

echo "✅ BRMAST CDC table and materialized view created successfully on cluster_1S_2R!"
