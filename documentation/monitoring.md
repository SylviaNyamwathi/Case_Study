# Monitoring & Observability

The brief asks what would be monitored in production, what triggers an alert,
and does not require the infrastructure. Everything below reads from artefacts
the pipeline already writes, `audit.dq_run_log`, `audit.ingestion_manifest`,
and `gold.mart_data_quality_summary`, so none of it depends on log scraping.

## What is instrumented today

| Signal | Emitted where |
|---|---|
| Source, Bronze, Silver-valid, Silver-rejected row counts | `audit.dq_run_log`, every run |
| `reconciled` boolean | `dq_run_log`, and the job exits non-zero on failure |
| `rejected_pct` and per-reason breakdown | `dq_run_log` + `mart_data_quality_summary` |
| `duplicates_removed` | `dq_run_log` |
| `multi_variant_quote_groups` | `dq_run_log` (source-structure change canary) |
| File hash, name, row count, `_batch_id`, ingestion time | `audit.ingestion_manifest` |
| Schema drift | Raises `SchemaDriftError`; run fails before writing |
| dbt test results | `dbt build` exit code + `target/run_results.json` |

---

## Metrics to monitor

### Freshness / arrival
- Time of file arrival vs the 06:00 SLA
- Age of the newest `as_of_date` in the fact
- dbt source freshness on `_ingested_at` (warn 26 h, error 48 h, configured in
  `_staging__sources.yml`)

### Volume
- Source row count per run, and its deviation from the trailing 7-day median
- Row counts per layer, and the deltas between them
- Partition count and size in Bronze and Silver

### Quality
- `rejected_pct` per run
- Rejection count per reason (a *new* reason appearing matters more than the
  total moving)
- `duplicates_removed`
- dbt test pass/fail count, and which test failed
- Schema drift events

### Pipeline health
- Runtime per layer, vs the trailing average
- Spark stage failures and retries
- Task retry count; consecutive failure count

### Warehouse
- `SVV_TABLE_INFO`: `skew_rows`, `unsorted`, `stats_off`, `size`
- Query runtime on the three marts (the BI-facing SLO)
- `STL_LOAD_ERRORS` after every `COPY`

### Business plausibility
These catch the failures that pass every technical test. A run can be perfectly
reconciled and still be wrong.
- Average fare per cabin vs the trailing average (Economy ≈ ₹6,572,
  Business ≈ ₹52,540 as the day-one baseline)
- Route count = 30, airline count = 6
- Any route losing all its carriers

---

## Alert conditions

| Severity | Condition | Action |
|---|---|---|
| **P1 page** | Reconciliation failed (`valid + rejected != bronze`) | Halt; do not publish to Redshift |
| **P1 page** | Schema drift, column added or removed | Halt; a human decides whether to widen the schema |
| **P1 page** | No file by 08:00 (SLA + 2 h) | Chase the source system |
| **P1 page** | Any dbt test failure on the fact or a mart | Block publication; the marts serve dashboards |
| **P2 alert** | `rejected_pct > 2%` | Investigate before the next run |
| **P2 alert** | Row count deviates >20% from the 7-day median | Investigate; likely a partial file |
| **P2 alert** | A rejection reason appears that has never fired before | Investigate the source change |
| **P2 alert** | Avg fare per cabin moves >30% day over day | Verify against the source; probably real, occasionally not |
| **P3 ticket** | Layer runtime >2× trailing average | Capacity review |
| **P3 ticket** | `skew_rows > 2` or `unsorted > 20%` on the fact | Revisit DISTKEY; schedule `VACUUM` |
| **P3 ticket** | Duplicate file delivered (run skipped) | Note it; repeated occurrences mean an upstream retry loop |
| **P3 ticket** | `multi_variant_quote_groups` moves >50% | Source fare structure may have changed — re-verify the business key |

Two thresholds deserve their reasoning stated, because the panel will ask.

**Why 2% rejected.** Day one rejected 0%, and the injected-fault demo rejected
0.45%. Anything above 2% on a file this clean means a systematic upstream change, 
a renamed category, a unit change — not a handful of bad rows. Tightening it
to 0% would page on a single legitimately odd row; loosening it to 10% would let
a broken carrier feed through unnoticed.

**Why 20% volume deviation.** With one file a day and a stable network, day-over-
day row count should barely move. 20% is wide enough to survive a genuine
schedule expansion and narrow enough to catch a truncated file, the most common
real-world failure and the one that reconciliation alone cannot catch, because a
half-file reconciles perfectly against itself.

---

## Production tooling

| Concern | Tool |
|---|---|
| Orchestration, retries, SLA misses | Airflow (`sla_miss_callback`) or Step Functions |
| Metrics and alarms | CloudWatch custom metrics from `dq_run_log`; SNS → PagerDuty/Slack |
| Freshness | `dbt source freshness` on a schedule, separate from the build |
| Test history | `dbt build --store-failures` — failing rows land in a table, so an analyst can see *which* rows failed, not just that a test failed |
| Lineage and docs | `dbt docs generate` published to S3 static hosting |
| Warehouse health | Scheduled `SVV_TABLE_INFO` snapshot into `audit` |
| Executive view | A dashboard over `mart_data_quality_summary` and `dq_run_log` |

The one non-obvious recommendation: `--store-failures`. "Test X failed" starts an
investigation; "here are the 14 rows that failed test X" ends it.

---

## Runbook: the four failures worth pre-writing

**Reconciliation failed.** The counts are in `dq_run_log`. Compare
`bronze_row_count` against `valid + rejected` for the batch, then check whether
Silver crashed mid-write, leaving a partial partition. Fix: rerun that
`as_of_date`. Dynamic overwrite makes the rerun safe, it replaces the partition
rather than adding to it.

**Schema drift.** Read the error; it names the missing or unexpected columns.
Decide with the source owner whether it is intentional. If yes, update
`SOURCE_SCHEMA`, `ACCEPTED_VALUES` if relevant, and the staging model, then
backfill. If no, quarantine the file. Never widen the schema to make an alert
stop.

**Rejected % spike.** Group `mart_data_quality_summary` by `rejection_reason`
for the snapshot. One dominant reason means one upstream change, usually a new
category value. Rejected rows are still on disk with their reasons, so nothing
needs re-extracting from the source.

**File never arrived.** Check `ingestion_manifest` for the last successful
`as_of_date`. The marts continue serving the last good snapshot, so this is a
staleness incident rather than an outage, the alert exists so nobody discovers
it from a dashboard that quietly stopped moving.
