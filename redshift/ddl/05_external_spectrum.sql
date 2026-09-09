-- ============================================================================
-- 05 - External schema (Redshift Spectrum) and the incremental load pattern
--
-- Two ways the Silver Parquet gets into Redshift. Both are here because the
-- right answer changes with volume, and the brief asks how the design behaves
-- as data grows.
--
--   A) Spectrum external table over S3
--      Zero load step. dbt models read the external table exactly as they read
--      Silver locally today - only the source location changes. Partition
--      pruning on as_of_date keeps scan cost proportional to the date range.
--      Best while the fact is small, and permanently best for cold history.
--
--   B) COPY into a staging table, then MERGE into the fact
--      Costs a load step but gives sorted, compressed, local storage and
--      predictable BI latency. This is what runs daily once the fact is large.
--
-- The pipeline uses B for the recent window and keeps A over the full history,
-- which is the "hot table + cold Spectrum" split described in
-- documentation/redshift_design.md.
-- ============================================================================

-- ---------------------------------------------------------------- OPTION A
CREATE EXTERNAL SCHEMA IF NOT EXISTS silver_ext
FROM DATA CATALOG
DATABASE 'kenya_airways_silver'
IAM_ROLE 'arn:aws:iam::<ACCOUNT_ID>:role/RedshiftSpectrumRole'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

CREATE EXTERNAL TABLE silver_ext.flights_valid (
    flight_quote_sk     VARCHAR(32),
    quote_group_sk      VARCHAR(32),
    airline             VARCHAR(64),
    flight              VARCHAR(16),
    source_city         VARCHAR(64),
    destination_city    VARCHAR(64),
    route               VARCHAR(129),
    departure_time      VARCHAR(32),
    arrival_time        VARCHAR(32),
    stops               VARCHAR(16),
    stops_count         INT,
    is_nonstop          BOOLEAN,
    class               VARCHAR(16),
    duration            DOUBLE PRECISION,
    duration_minutes    INT,
    days_left           INT,
    price_inr           DECIMAL(12,2),
    _source_file        VARCHAR(256),
    _source_row         INT,
    _ingested_at        TIMESTAMP,
    _batch_id           VARCHAR(64)
)
PARTITIONED BY (as_of_date DATE)
STORED AS PARQUET
LOCATION 's3://<BUCKET>/lake/silver/flights_valid/';

-- Each daily run registers its own partition. Without this the new files are
-- invisible to Spectrum - a silent "yesterday's data is missing" failure, so
-- this statement belongs in the pipeline, not in a runbook.
ALTER TABLE silver_ext.flights_valid
    ADD IF NOT EXISTS PARTITION (as_of_date = '2026-09-09')
    LOCATION 's3://<BUCKET>/lake/silver/flights_valid/as_of_date=2026-09-09/';


-- ---------------------------------------------------------------- OPTION B
-- Staging table mirrors the fact's structure but is DISTSTYLE EVEN: it is
-- written once, read once, and never joined, so co-location buys nothing.
DROP TABLE IF EXISTS staging.fct_flight_price_quote_stg;
CREATE TABLE staging.fct_flight_price_quote_stg (LIKE gold.fct_flight_price_quote);
ALTER TABLE staging.fct_flight_price_quote_stg ALTER DISTSTYLE EVEN;

TRUNCATE staging.fct_flight_price_quote_stg;

COPY staging.fct_flight_price_quote_stg
FROM 's3://<BUCKET>/lake/gold/fct_flight_price_quote/as_of_date=2026-09-09/'
IAM_ROLE 'arn:aws:iam::<ACCOUNT_ID>:role/RedshiftCopyRole'
FORMAT AS PARQUET;

-- MERGE, not INSERT. This is the idempotency guarantee: rerunning the same
-- as_of_date replaces its own rows instead of appending them a second time.
-- Because as_of_date is part of flight_quote_sk, a genuinely new snapshot of
-- the same flight inserts a new row (building price history) while a rerun of
-- the same snapshot updates in place. That is what makes reruns safe and
-- late-arriving files harmless.
BEGIN TRANSACTION;

MERGE INTO gold.fct_flight_price_quote
USING staging.fct_flight_price_quote_stg AS stg
    ON gold.fct_flight_price_quote.flight_quote_sk = stg.flight_quote_sk
WHEN MATCHED THEN UPDATE SET
    price_inr           = stg.price_inr,
    price_per_hour_inr  = stg.price_per_hour_inr,
    duration_hours      = stg.duration_hours,
    duration_minutes    = stg.duration_minutes,
    source_file_name    = stg.source_file_name,
    ingested_at         = stg.ingested_at,
    batch_id            = stg.batch_id
WHEN NOT MATCHED THEN INSERT VALUES (
    stg.flight_quote_sk, stg.quote_group_sk, stg.as_of_date, stg.route_id,
    stg.airline_id, stg.flight_number, stg.airline_name, stg.route_name,
    stg.origin_city, stg.destination_city, stg.departure_time_bucket,
    stg.arrival_time_bucket, stg.stops_label, stg.stops_count, stg.is_nonstop,
    stg.cabin_class, stg.is_premium_cabin, stg.lead_time_bucket,
    stg.lead_time_bucket_sort, stg.days_to_departure, stg.price_inr,
    stg.price_per_hour_inr, stg.duration_hours, stg.duration_minutes,
    stg.source_file_name, stg.ingested_at, stg.batch_id
);

COMMIT;

-- Post-load maintenance. Daily loads arrive in as_of_date order (the leading
-- sort key), so the table stays nearly sorted and VACUUM is usually a no-op -
-- run it on a schedule rather than every load, and always ANALYZE so the
-- planner's row estimates keep up with the new partition.
ANALYZE gold.fct_flight_price_quote;
-- VACUUM DELETE ONLY gold.fct_flight_price_quote;   -- weekly
-- VACUUM REINDEX gold.fct_flight_price_quote;       -- only after a backfill


-- ------------------------------------------------------- OPERATIONAL CHECKS
-- Distribution skew and unsorted %. If skew_rows drifts above ~2, the DISTKEY
-- choice needs revisiting (see the scaling note in 03_fact.sql).
SELECT
    "table",
    size            AS size_mb,
    diststyle,
    sortkey1,
    skew_rows,
    unsorted        AS unsorted_pct,
    stats_off,
    tbl_rows
FROM svv_table_info
WHERE schema IN ('gold', 'audit')
ORDER BY size DESC;

-- Source-to-target reconciliation, in the warehouse rather than in the job:
-- the manifest's source_row_count must equal what actually landed.
SELECT
    m.as_of_date,
    m.source_row_count,
    COUNT(f.flight_quote_sk)                            AS fact_row_count,
    m.source_row_count - COUNT(f.flight_quote_sk)       AS variance
FROM audit.ingestion_manifest m
LEFT JOIN gold.fct_flight_price_quote f
    ON f.as_of_date = m.as_of_date
GROUP BY m.as_of_date, m.source_row_count
HAVING m.source_row_count <> COUNT(f.flight_quote_sk);
