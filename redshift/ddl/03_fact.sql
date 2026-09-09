-- ============================================================================
-- 03 - Fact table
--
-- GRAIN (identical wording in the README, the dbt model and the docs):
--   One row = one price quote for one flight number, on one directional route,
--   in one cabin class, at one booking lead time (days_to_departure), captured
--   in one price snapshot (as_of_date).
--
-- KEY DESIGN DECISIONS
--
-- DISTKEY (route_id)
--   Every mart aggregates by route, so co-locating rows of the same route on
--   the same slice keeps those GROUP BYs local. route_id has 30 distinct values
--   across 300k rows today - even distribution, no slice skew. The alternative,
--   DISTKEY(as_of_date), would be the worst possible choice: one snapshot per
--   day means every day's load lands entirely on one slice.
--   As the data grows past a few hundred million rows and route cardinality
--   stays at 30, revisit: at that point DISTSTYLE EVEN plus the ALL dimensions
--   spreads scan work better than 30 fat buckets. See the scaling note in
--   documentation/redshift_design.md.
--
-- SORTKEY (as_of_date, route_id, cabin_class)
--   COMPOUND, leading on as_of_date, because essentially every query filters
--   on a date range - that filter alone lets Redshift skip whole 1MB blocks
--   via zone maps. route_id and cabin_class follow as the next most common
--   filters. Leading on as_of_date also means daily appends arrive in sort
--   order, so the table stays nearly sorted and VACUUM stays cheap.
--
-- Encodings
--   Left to AUTO (Redshift's default) except the sort key. as_of_date is
--   declared RAW deliberately: compressing the leading sort key degrades zone-
--   map effectiveness, which is the one thing this table's performance rests on.
--
-- Why no IDENTITY surrogate key
--   flight_quote_sk is a deterministic md5 of the business key, computed in
--   Silver. Deterministic keys survive reruns and full refreshes unchanged; an
--   IDENTITY column would issue new keys on every reload and break any BI
--   bookmark or downstream join that stored the old value.
-- ============================================================================

DROP TABLE IF EXISTS gold.fct_flight_price_quote CASCADE;
CREATE TABLE gold.fct_flight_price_quote (
    -- keys
    flight_quote_sk         CHAR(32)        NOT NULL ENCODE RAW,
    quote_group_sk          CHAR(32)        NOT NULL,
    as_of_date              DATE            NOT NULL ENCODE RAW,
    route_id                CHAR(32)        NOT NULL,
    airline_id              CHAR(32)        NOT NULL,

    -- degenerate / descriptive dimensions
    flight_number           VARCHAR(16)     NOT NULL,
    airline_name            VARCHAR(64)     NOT NULL,
    route_name              VARCHAR(129)    NOT NULL,
    origin_city             VARCHAR(64)     NOT NULL,
    destination_city        VARCHAR(64)     NOT NULL,
    departure_time_bucket   VARCHAR(32)     NOT NULL,
    arrival_time_bucket     VARCHAR(32)     NOT NULL,
    stops_label             VARCHAR(16)     NOT NULL,
    stops_count             SMALLINT        NOT NULL,
    is_nonstop              BOOLEAN         NOT NULL,
    cabin_class             VARCHAR(16)     NOT NULL,
    is_premium_cabin        BOOLEAN         NOT NULL,
    lead_time_bucket        VARCHAR(64)     NOT NULL,
    lead_time_bucket_sort   SMALLINT        NOT NULL,
    days_to_departure       SMALLINT        NOT NULL,

    -- measures
    price_inr               DECIMAL(12,2)   NOT NULL,
    price_per_hour_inr      DECIMAL(14,2),
    duration_hours          DOUBLE PRECISION NOT NULL,
    duration_minutes        INTEGER         NOT NULL,

    -- lineage: which file, which run, when. Cheap to store, decisive when a
    -- number is disputed three weeks later.
    source_file_name        VARCHAR(256)    NOT NULL,
    ingested_at             TIMESTAMP       NOT NULL,
    batch_id                VARCHAR(64)     NOT NULL,

    PRIMARY KEY (flight_quote_sk),
    FOREIGN KEY (route_id)   REFERENCES gold.dim_route (route_id),
    FOREIGN KEY (airline_id) REFERENCES gold.dim_airline (airline_id)
)
DISTSTYLE KEY
DISTKEY (route_id)
COMPOUND SORTKEY (as_of_date, route_id, cabin_class);

COMMENT ON TABLE gold.fct_flight_price_quote IS
    'Grain: one price quote per flight number x directional route x cabin class '
    'x booking lead time x snapshot date (as_of_date).';
COMMENT ON COLUMN gold.fct_flight_price_quote.flight_quote_sk IS
    'md5 of flight + origin + destination + time buckets + class + days_left + '
    'duration + price + as_of_date. duration and price are IN the key because '
    'the source lists multiple genuine fare variants per flight per lead time - '
    'omitting them silently deletes 21.5% of rows.';
COMMENT ON COLUMN gold.fct_flight_price_quote.quote_group_sk IS
    'Groups fare variants of the same flight in the same lead-time bucket. '
    'Deliberately NOT unique.';
COMMENT ON COLUMN gold.fct_flight_price_quote.as_of_date IS
    'Logical price-snapshot date, minted at ingestion (source has no date '
    'column - see documentation/assumptions.md).';


-- ---------------------------------------------------------------------------
-- Audit tables. Small, but they are what makes "can you prove this number?"
-- answerable.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS audit.ingestion_manifest;
CREATE TABLE audit.ingestion_manifest (
    source_file         VARCHAR(256)    NOT NULL,
    file_hash           CHAR(32)        NOT NULL,   -- md5 of file contents
    as_of_date          DATE            NOT NULL,
    batch_id            VARCHAR(64)     NOT NULL,
    source_row_count    BIGINT          NOT NULL,
    ingested_at         TIMESTAMP       NOT NULL,
    PRIMARY KEY (file_hash, as_of_date)
)
DISTSTYLE ALL
SORTKEY (as_of_date);

COMMENT ON TABLE audit.ingestion_manifest IS
    'One row per accepted file. The (file_hash, as_of_date) key is the '
    'duplicate-file-delivery guard: a re-sent file, even renamed, is a no-op.';


DROP TABLE IF EXISTS audit.dq_run_log;
CREATE TABLE audit.dq_run_log (
    layer                       VARCHAR(16)     NOT NULL,
    batch_id                    VARCHAR(64),
    as_of_date                  VARCHAR(16),
    source_file                 VARCHAR(256),
    source_row_count            BIGINT,
    bronze_row_count            BIGINT,
    silver_valid_count          BIGINT,
    silver_rejected_count       BIGINT,
    duplicates_removed          BIGINT,
    multi_variant_quote_groups  BIGINT,
    rejected_pct                DECIMAL(9,4),
    reconciled                  BOOLEAN,
    rejection_reason_breakdown  VARCHAR(4096),  -- JSON
    logged_at                   TIMESTAMP
)
DISTSTYLE ALL
SORTKEY (logged_at);

COMMENT ON TABLE audit.dq_run_log IS
    'Per-run DQ metrics. Source of the monitoring dashboard and the >2% '
    'rejected-rate alert in documentation/monitoring.md.';
