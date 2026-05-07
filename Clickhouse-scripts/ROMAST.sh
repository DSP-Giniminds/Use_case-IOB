#!/bin/bash
# ============================================================
# Script: ro.sh
# Purpose: CDC ingestion for ROMAST
# Handles insert, update, and delete in real time
# ============================================================

CLICKHOUSE_HOST="192.168.80.130"
CLICKHOUSE_PORT="9000"
USER="admin"
PASSWORD="Sruj@n13"

clickhouse-client --host "$CLICKHOUSE_HOST" --port "$CLICKHOUSE_PORT" --user="$USER" --password="$PASSWORD" --multiquery <<'EOSQL'

-- 1️⃣ Target Table (CDC + replication + update/delete safe)
CREATE TABLE IF NOT EXISTS IOB1."ROMAST" ON CLUSTER default
(
    RO_CODE String,
    REGION_NAME String,
    updated_at DateTime DEFAULT now(),
    _deleted UInt8 DEFAULT 0
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/{database}/{table}',
    '{replica}',
    updated_at
)
ORDER BY RO_CODE;

-- 2️⃣ Kafka Engine Table (Debezium CDC messages)
CREATE TABLE IF NOT EXISTS IOB1."ROMAST_KAFKA" ON CLUSTER default
(
    before JSON,
    after JSON,
    op String,
    ts_ms UInt64
)
ENGINE = Kafka
SETTINGS kafka_broker_list = '192.168.80.130:9094,192.168.80.131:9094',
         kafka_topic_list = 'test296.C__IOB1.ROMAST',
         kafka_group_name = 'ROMAST_011',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 2;

-- 3️⃣ Materialized View (map CDC → final table)
CREATE MATERIALIZED VIEW IF NOT EXISTS IOB1."ROMAST_MV" ON CLUSTER default
TO IOB1."ROMAST"
AS
SELECT
    if(op = 'd', JSONExtractString(toString(before), 'RO_CODE'), JSONExtractString(toString(after), 'RO_CODE')) AS RO_CODE,
    if(op = 'd', JSONExtractString(toString(before), 'REGION_NAME'), JSONExtractString(toString(after), 'REGION_NAME')) AS REGION_NAME,
    if(ts_ms > 0, toDateTime(intDiv(ts_ms, 1000)), now()) AS updated_at,
    if(op = 'd', 1, 0) AS _deleted
FROM IOB1."ROMAST_KAFKA";

EOSQL

echo "✅ ROMAST CDC ingestion (ro.sh) setup completed successfully on CLUSTER default!"
