-- ============================================================================
-- 04 - Gold marts
--
-- All three business marts are aggregates of gold.fct_flight_price_quote and
-- are built by dbt (materialized='table'). The DDL is here so the physical
-- design is explicit and reviewable rather than left to dbt's defaults.
--
-- Common pattern: DISTKEY(route_id) matches the fact, so building a mart from
-- the fact is a local aggregation with no redistribution. Sort keys lead on
-- as_of_date for the same zone-map reason as the fact.
-- ============================================================================

-- ------------------------------------------------------------------ MART 1
-- Route performance. Grain: route x cabin class x snapshot.
-- Business value: shows where a carrier holds a price premium (few
-- competitors, high average fare) and where competition has compressed fares.
-- price_spread_inr and price_stddev_inr matter more than the average - a wide
-- spread on a busy route is where revenue management has room to move.
DROP TABLE IF EXISTS gold.mart_route_performance;
CREATE TABLE gold.mart_route_performance (
    as_of_date              DATE            NOT NULL ENCODE RAW,
    route_id                CHAR(32)        NOT NULL,
    route_name              VARCHAR(129)    NOT NULL,
    origin_city             VARCHAR(64)     NOT NULL,
    destination_city        VARCHAR(64)     NOT NULL,
    cabin_class             VARCHAR(16)     NOT NULL,

    quote_count             BIGINT          NOT NULL,
    airline_count           INTEGER         NOT NULL,
    flight_number_count     INTEGER         NOT NULL,

    avg_price_inr           DECIMAL(14,2),
    min_price_inr           DECIMAL(14,2),
    max_price_inr           DECIMAL(14,2),
    price_spread_inr        DECIMAL(14,2),
    price_stddev_inr        DECIMAL(14,2),
    max_to_min_price_ratio  DECIMAL(14,2),

    avg_duration_hours      DECIMAL(14,2),
    fastest_duration_hours  DOUBLE PRECISION,
    avg_price_per_hour_inr  DECIMAL(14,2),
    nonstop_share_pct       DECIMAL(9,2),

    PRIMARY KEY (as_of_date, route_id, cabin_class),
    FOREIGN KEY (route_id) REFERENCES gold.dim_route (route_id)
)
DISTSTYLE KEY
DISTKEY (route_id)
COMPOUND SORTKEY (as_of_date, route_id, cabin_class);


-- ------------------------------------------------------------------ MART 2
-- Booking lead-time pricing. Grain: route x cabin x lead-time band x snapshot.
-- Business value: the fare curve as departure approaches, indexed against the
-- cheapest band on the same route and cabin, so the shape is readable without
-- the analyst computing a baseline. Drives advance-purchase fencing and
-- campaign timing.
DROP TABLE IF EXISTS gold.mart_booking_leadtime_pricing;
CREATE TABLE gold.mart_booking_leadtime_pricing (
    as_of_date                      DATE            NOT NULL ENCODE RAW,
    route_id                        CHAR(32)        NOT NULL,
    route_name                      VARCHAR(129)    NOT NULL,
    cabin_class                     VARCHAR(16)     NOT NULL,
    lead_time_bucket                VARCHAR(64)     NOT NULL,
    lead_time_bucket_sort           SMALLINT        NOT NULL,

    quote_count                     BIGINT          NOT NULL,
    min_days_to_departure           SMALLINT,
    max_days_to_departure           SMALLINT,

    avg_price_inr                   DECIMAL(14,2),
    min_price_inr                   DECIMAL(14,2),
    max_price_inr                   DECIMAL(14,2),
    avg_price_per_hour_inr          DECIMAL(14,2),
    cheapest_band_avg_price_inr     DECIMAL(14,2),
    price_index_vs_cheapest_band    DECIMAL(14,2),  -- 100 = cheapest band
    pct_uplift_vs_advance_band      DECIMAL(14,2),

    PRIMARY KEY (as_of_date, route_id, cabin_class, lead_time_bucket),
    FOREIGN KEY (route_id)         REFERENCES gold.dim_route (route_id),
    FOREIGN KEY (lead_time_bucket) REFERENCES gold.dim_lead_time_bucket (lead_time_bucket)
)
DISTSTYLE KEY
DISTKEY (route_id)
COMPOUND SORTKEY (as_of_date, route_id, lead_time_bucket_sort);


-- ------------------------------------------------------------------ MART 3
-- Airline & cabin mix. Grain: airline x route x snapshot.
-- Business value: competitive share of quoted flights per carrier, plus the
-- Business-over-Economy premium on the same route. High share with a thin
-- premium = money left on the table; thin share with a fat premium = niche
-- premium product. business_premium_pct is NULL, never 0, where a carrier sells
-- only one cabin.
DROP TABLE IF EXISTS gold.mart_airline_cabin_mix;
CREATE TABLE gold.mart_airline_cabin_mix (
    as_of_date              DATE            NOT NULL ENCODE RAW,
    route_id                CHAR(32)        NOT NULL,
    route_name              VARCHAR(129)    NOT NULL,
    airline_name            VARCHAR(64)     NOT NULL,

    quote_count             BIGINT          NOT NULL,
    flight_number_count     INTEGER         NOT NULL,
    economy_quote_count     BIGINT          NOT NULL,
    business_quote_count    BIGINT          NOT NULL,

    avg_economy_price_inr   DECIMAL(14,2),
    avg_business_price_inr  DECIMAL(14,2),
    avg_price_inr           DECIMAL(14,2),
    nonstop_share_pct       DECIMAL(9,2),
    route_share_pct         DECIMAL(9,2)    NOT NULL,
    business_premium_pct    DECIMAL(14,2),
    sells_business_cabin    BOOLEAN         NOT NULL,

    PRIMARY KEY (as_of_date, route_id, airline_name),
    FOREIGN KEY (route_id) REFERENCES gold.dim_route (route_id)
)
DISTSTYLE KEY
DISTKEY (route_id)
COMPOUND SORTKEY (as_of_date, route_id, airline_name);


-- --------------------------------------------------- SUPPORTING (DQ) MART
-- Grain: snapshot x rejection reason. Not a business mart - this is what the
-- monitoring dashboard points at. DISTSTYLE ALL: it stays small and is joined
-- against date spines for trend charts.
DROP TABLE IF EXISTS gold.mart_data_quality_summary;
CREATE TABLE gold.mart_data_quality_summary (
    as_of_date                      DATE            NOT NULL,
    rejection_reason                VARCHAR(256)    NOT NULL,
    rejected_row_count_for_reason   BIGINT          NOT NULL,
    accepted_row_count              BIGINT          NOT NULL,
    rejected_row_count_total        BIGINT          NOT NULL,
    bronze_row_count_reconciled     BIGINT          NOT NULL,
    rejected_pct                    DECIMAL(9,2)    NOT NULL,
    PRIMARY KEY (as_of_date, rejection_reason)
)
DISTSTYLE ALL
SORTKEY (as_of_date);
