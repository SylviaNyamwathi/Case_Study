# Amazon Redshift: Design Decisions & Scaling

DDL is in `redshift/ddl/`. This document explains the choices and how they
behave as volume grows, which is what section 7 of the brief actually asks for.

## Distribution

| Table | Style | Key | Reason |
|---|---|---|---|
| `fct_flight_price_quote` | `KEY` | `route_id` | Every mart aggregates by route; co-location keeps those GROUP BYs local |
| `dim_airline` (6 rows) | `ALL` | — | Replicated to every node; joins never redistribute |
| `dim_route` (30 rows) | `ALL` | — | Same |
| `dim_lead_time_bucket` (5 rows) | `ALL` | — | Same |
| `mart_*` | `KEY` | `route_id` | Matches the fact, so building a mart from it needs no redistribution |
| `*_stg` (load staging) | `EVEN` | — | Written once, read once, never joined, co-location buys nothing |
| `audit.*` | `ALL` | — | Small control tables, joined against everything |

**Why not `DISTKEY(as_of_date)`.** It would be the worst available choice. One
snapshot per day means every day's load lands entirely on one slice: a single
slice does all the write work, and any single-date query, which is most of
them, runs on one slice while the rest idle.

**Why not `DISTSTYLE ALL` on the fact.** It grows without bound; replicating it
multiplies storage by the node count and makes every write N times more
expensive.

**Skew, checked not assumed.** `route_id` has 30 distinct values over 300,153
rows - 10,005 rows each, near-perfectly even. The monitoring query in
`05_external_spectrum.sql` reports `skew_rows` from `SVV_TABLE_INFO`; above ~2
the choice needs revisiting.

---

## Sort keys

`COMPOUND SORTKEY (as_of_date, route_id, cabin_class)` on the fact.

**Leading on `as_of_date`** because effectively every query filters on a date
range. Redshift's zone maps store min/max per 1 MB block, so a date filter skips
whole blocks without reading them, the single largest performance lever on this
table. It also means daily appends arrive *in sort order*, so the table stays
nearly sorted and `VACUUM` is usually a no-op.

**`route_id`, `cabin_class` next** as the next most common filters.

**Compound, not interleaved.** Interleaved sort keys give equal weight to
several columns and suit unpredictable filter patterns, but they need frequent
`VACUUM REINDEX` and degrade badly on append-heavy tables. Here the filter
pattern is predictable and date-led, which is exactly the compound case.

**`as_of_date` and `flight_quote_sk` are `ENCODE RAW`.** Compressing the leading
sort key degrades zone-map effectiveness, which is the one thing this table's
performance rests on. Everything else is left to `AUTO`, Redshift picks better
encodings from actual data than a guess in a DDL file.

---

## Keys

Primary and foreign keys are declared but, as Redshift documents, **not
enforced**. They are declared anyway because the query planner uses them to
eliminate redundant joins and to produce better row estimates. Enforcement lives
in dbt tests (`unique`, `not_null`, `relationships`), where a violation is a
readable message in CI rather than a load abort at 3 a.m.

**No `IDENTITY` surrogate keys.** `flight_quote_sk` is a deterministic md5 of
the business key, computed once in Silver. Deterministic keys survive reruns and
full refreshes unchanged; `IDENTITY` would mint new values on every reload and
break any stored BI bookmark or downstream join.

---

## Loading

Two paths, both in `05_external_spectrum.sql`, because the right answer changes
with volume.

**Spectrum external schema over S3.** Zero load step; dbt models read the
external table exactly as they read Silver locally. Partition pruning on
`as_of_date` keeps scan cost proportional to the date range. Best while the fact
is small, and permanently best for cold history. The daily
`ALTER TABLE ... ADD PARTITION` belongs *in the pipeline*, an unregistered
partition is invisible to Spectrum, which is a silent "yesterday's data is
missing" failure.

**`COPY` into staging, then `MERGE`.** Costs a load step but gives sorted,
compressed, local storage and predictable BI latency. `MERGE` on
`flight_quote_sk` is what makes reruns idempotent: a reprocessed snapshot
updates in place instead of appending a second copy.

Post-load: always `ANALYZE` (the planner needs current stats for the new
partition); `VACUUM DELETE ONLY` weekly; `VACUUM REINDEX` only after a backfill.

---

## Scaling

Today: 300,153 rows, ~25 MB CSV, one snapshot. A daily file of this size means
roughly **110 M rows a year**.

| Volume | What holds | What changes |
|---|---|---|
| < 10 M rows | Everything as designed | Nothing |
| 10–100 M | `DISTKEY(route_id)` still even; zone maps carry date queries | Marts become incremental on `as_of_date`; add `VACUUM` to the weekly schedule |
| 100 M–1 B | Fact stays local for a hot window (12–24 months) | Move older snapshots to Spectrum external tables; add a union view over hot + cold. Reconsider `DISTSTYLE EVEN` with route cardinality fixed at 30, 30 fat buckets eventually spread scan work worse than EVEN |
| > 1 B | — | Serverless or an RA3 resize; consider materialized views for the marts; partition the S3 lake by `as_of_date/route` for finer pruning |

**The constraint that actually bites first** is not row count, it is that route
cardinality is fixed at 30 while the fact grows without limit. Distribution keys
work on the ratio between key cardinality and slice count, so a key with 30
values stops being a good distribution choice long before the data gets large.
That is why `skew_rows` is on the monitoring list rather than something to check
once and forget.

**Cost control.** Concurrency scaling for BI bursts; pause/resume on
non-production clusters; Spectrum for cold history so it sits in S3 at S3
prices rather than in cluster storage.
