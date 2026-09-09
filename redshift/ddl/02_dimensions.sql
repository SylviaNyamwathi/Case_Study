-- ============================================================================
-- 02 - Dimensions
--
-- Both dimensions are tiny (6 and 30 rows). DISTSTYLE ALL replicates them to
-- every compute node, so a join from the fact never redistributes data across
-- the network. The storage cost of replicating 30 rows is irrelevant; the cost
-- of a broadcast-less shuffle on a 100M-row fact is not.
--
-- Primary/foreign keys are declared even though Redshift does not enforce them:
-- the query planner uses them to eliminate redundant joins and to pick better
-- plans. Enforcement stays where it belongs - dbt tests.
-- ============================================================================

DROP TABLE IF EXISTS gold.dim_airline CASCADE;
CREATE TABLE gold.dim_airline (
    airline_id              CHAR(32)        NOT NULL,   -- md5 hex, fixed width
    airline_name            VARCHAR(64)     NOT NULL,
    airline_display_name    VARCHAR(64)     NOT NULL,
    offers_business_class   BOOLEAN         NOT NULL,
    quote_count             BIGINT          NOT NULL,
    route_count             INTEGER         NOT NULL,
    flight_number_count     INTEGER         NOT NULL,
    avg_price_inr           DECIMAL(14,2),
    min_price_inr           DECIMAL(14,2),
    max_price_inr           DECIMAL(14,2),
    PRIMARY KEY (airline_id)
)
DISTSTYLE ALL
SORTKEY (airline_name);

COMMENT ON TABLE gold.dim_airline IS
    'One row per carrier (6). DISTSTYLE ALL - replicated, never shuffled.';


DROP TABLE IF EXISTS gold.dim_route CASCADE;
CREATE TABLE gold.dim_route (
    route_id                    CHAR(32)    NOT NULL,
    origin_city                 VARCHAR(64) NOT NULL,
    destination_city            VARCHAR(64) NOT NULL,
    route_name                  VARCHAR(129) NOT NULL,  -- 'Origin-Destination'
    route_pair_name             VARCHAR(131),           -- 'A<->B', bidirectional
    competing_airline_count     INTEGER,
    has_nonstop_service         BOOLEAN,
    fastest_duration_hours      DOUBLE PRECISION,
    quote_count                 BIGINT,
    PRIMARY KEY (route_id)
)
DISTSTYLE ALL
SORTKEY (origin_city, destination_city);

COMMENT ON TABLE gold.dim_route IS
    'One row per DIRECTIONAL route (30). Delhi->Mumbai and Mumbai->Delhi are '
    'separate rows on purpose: different demand curves, different fares.';


-- Lead-time band lookup. Materialised as a table rather than left as a CASE
-- expression so BI tools get a sort order (band_sort) and a joinable label
-- instead of alphabetising '0-3 days' after '16-30 days'.
DROP TABLE IF EXISTS gold.dim_lead_time_bucket CASCADE;
CREATE TABLE gold.dim_lead_time_bucket (
    lead_time_bucket        VARCHAR(64) NOT NULL,
    lead_time_bucket_sort   SMALLINT    NOT NULL,
    min_days_to_departure   SMALLINT    NOT NULL,
    max_days_to_departure   SMALLINT,               -- NULL = open-ended band
    PRIMARY KEY (lead_time_bucket)
)
DISTSTYLE ALL
SORTKEY (lead_time_bucket_sort);

INSERT INTO gold.dim_lead_time_bucket VALUES
    ('0-3 days (last minute)',  1,  0,  3),
    ('4-7 days (short)',        2,  4,  7),
    ('8-15 days (medium)',      3,  8, 15),
    ('16-30 days (long)',       4, 16, 30),
    ('31+ days (advance)',      5, 31, NULL);
