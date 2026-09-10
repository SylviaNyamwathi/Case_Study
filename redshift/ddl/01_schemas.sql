-- ============================================================================
-- Amazon Redshift - Gold layer DDL
-- Kenya Airways / airline flight pricing pipeline
--
-- Target: Redshift provisioned (ra3) or Serverless. No live cluster required -
--         design decisions are documented inline and in
--         documentation/redshift_design.md.
--
-- Run order: 01_schemas.sql -> 02_dimensions.sql -> 03_fact.sql -> 04_marts.sql
--            -> 05_external_spectrum.sql (optional)
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS audit;

COMMENT ON SCHEMA gold IS
    'Business-ready fact, dimensions and marts. The only schema BI tools query.';
COMMENT ON SCHEMA staging IS
    'Thin 1:1 views over the Silver external schema. Built by dbt, not queried directly.';
COMMENT ON SCHEMA audit IS
    'Pipeline control tables: ingestion manifest and per-run data-quality log.';

-- Read-only consumer role for BI. Granting on the schema (not table by table)
-- means new marts are visible without a new grant, which is the failure mode
-- that usually breaks a dashboard the morning after a release.
CREATE GROUP bi_readers;
GRANT USAGE ON SCHEMA gold TO GROUP bi_readers;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO GROUP bi_readers;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT ON TABLES TO GROUP bi_readers;
