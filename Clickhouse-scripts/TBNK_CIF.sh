#!/bin/bash
# ============================================================
# Script: Create ClickHouse Tables & Materialized View (CDC)
# Target: TBNK_CIF
# Cluster: default
# Description: Handles INSERT, UPDATE, and DELETE via Debezium CDC
# ============================================================

CLICKHOUSE_HOST="192.168.80.130"
CLICKHOUSE_PORT="9000"
USER="admin"
PASSWORD="Sruj@n13"


clickhouse-client --host "$CLICKHOUSE_HOST" --port "$CLICKHOUSE_PORT" --user="$USER" --password="$PASSWORD" --multiquery <<'EOSQL'

-- ============================================================
-- 1️⃣ Target Table: Stores latest version by TXN_ID
-- ============================================================
CREATE TABLE IF NOT EXISTS IOB1."TBNK_CIF" ON CLUSTER default
(
    TXN_ID               String,
    REQ_REF              String,
    BANKREF              String,
    REQUESTID            String,
    USERID               String,
    EDUQUALIFICATION     String,
    INCOMESOURCE         String,
    TOTALINCOME          String,
    EMPLOYMENTSTATUS     String,
    BRCODE               String,
    TXN_DT               String,
    STATUS               String,
    updated_at           DateTime,
    _deleted             UInt8 DEFAULT 0
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/{database}/{table}',
    '{replica}',
    updated_at
)
ORDER BY TXN_ID;

-- ============================================================
-- 2️⃣ Kafka Engine Source Table (Debezium CDC Envelope)
-- ============================================================
CREATE TABLE IF NOT EXISTS IOB1."TBNK_CIF_KAFKA" ON CLUSTER default
(
    before JSON,
    after JSON,
    op String,
    ts_ms UInt64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list   = '192.168.80.130:9094,192.168.80.131:9094',
    kafka_topic_list    = 'test296.C__IOB1.TBNK_CIF',
    kafka_group_name    = 'TBNK_CIF_011',
    kafka_format        = 'JSONEachRow',
    kafka_num_consumers = 2;

-- ============================================================
-- 3️⃣ Materialized View: Parse JSON CDC → Target Table
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS IOB1."TBNK_CIF_MV" ON CLUSTER default
TO IOB1."TBNK_CIF"
AS
SELECT
    if(op = 'd',
       JSONExtractString(toString(before), 'TXN_ID'),
       JSONExtractString(toString(after), 'TXN_ID')
    ) AS TXN_ID,
    if(op = 'd',
       JSONExtractString(toString(before), 'REQ_REF'),
       JSONExtractString(toString(after), 'REQ_REF')
    ) AS REQ_REF,
    if(op = 'd',
       JSONExtractString(toString(before), 'BANKREF'),
       JSONExtractString(toString(after), 'BANKREF')
    ) AS BANKREF,
    if(op = 'd',
       JSONExtractString(toString(before), 'REQUESTID'),
       JSONExtractString(toString(after), 'REQUESTID')
    ) AS REQUESTID,
    if(op = 'd',
       JSONExtractString(toString(before), 'USERID'),
       JSONExtractString(toString(after), 'USERID')
    ) AS USERID,
    if(op = 'd',
       JSONExtractString(toString(before), 'EDUQUALIFICATION'),
       JSONExtractString(toString(after), 'EDUQUALIFICATION')
    ) AS EDUQUALIFICATION,
    if(op = 'd',
       JSONExtractString(toString(before), 'INCOMESOURCE'),
       JSONExtractString(toString(after), 'INCOMESOURCE')
    ) AS INCOMESOURCE,
    if(op = 'd',
       JSONExtractString(toString(before), 'TOTALINCOME'),
       JSONExtractString(toString(after), 'TOTALINCOME')
    ) AS TOTALINCOME,
    if(op = 'd',
       JSONExtractString(toString(before), 'EMPLOYMENTSTATUS'),
       JSONExtractString(toString(after), 'EMPLOYMENTSTATUS')
    ) AS EMPLOYMENTSTATUS,
    if(op = 'd',
       JSONExtractString(toString(before), 'BRCODE'),
       JSONExtractString(toString(after), 'BRCODE')
    ) AS BRCODE,
    if(op = 'd',
       JSONExtractString(toString(before), 'TXN_DT'),
       JSONExtractString(toString(after), 'TXN_DT')
    ) AS TXN_DT,
    if(op = 'd',
       JSONExtractString(toString(before), 'STATUS'),
       JSONExtractString(toString(after), 'STATUS')
    ) AS STATUS,
    if(ts_ms > 0, toDateTime(intDiv(ts_ms, 1000)), now()) AS updated_at,
    if(op = 'd', 1, 0) AS _deleted
FROM IOB1."TBNK_CIF_KAFKA";

EOSQL

echo "✅ CDC ingestion for TBNK_CIF (insert, update, delete) created successfully."
