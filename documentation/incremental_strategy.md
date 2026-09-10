# Incremental Processing Strategy

The brief assumes a new flight-pricing file arrives every day and asks that the
historical dataset not be rebuilt unnecessarily. Five scenarios are named. All
five are handled by one design decision plus one key choice.

## The two things everything else follows from

**1. `as_of_date` partitions every layer.** Bronze, Silver valid, Silver
rejected, and the fact are all partitioned or sorted by it. A day's work touches
one partition.

**2. `as_of_date` is *inside* the business key.** This is what makes reruns and
re-quotes different operations:

- Same flight, **new** snapshot date → different `flight_quote_sk` → **appends**
  a row, building a price history.
- Same flight, **same** snapshot date → same `flight_quote_sk` → **replaces**
  itself.

---

## Scenario 1: New records

A new file lands with a new `as_of_date`. Bronze writes one new partition;
existing partitions are never read or rewritten. Silver processes only that
snapshot (`--as-of-date 2026-09-10`). dbt's incremental fact filters to
snapshots at or after the newest one already loaded:

```sql
{% if is_incremental() %}
where as_of_date >= (
    select coalesce(max(as_of_date), '1900-01-01') from {{ this }}
)
{% endif %}
```

`>=` not `>` on purpose — it lets a same-day rerun correct itself instead of
skipping its own partition.

**Cost:** proportional to one day's file, not to history.

---

## Scenario 2: Changed records

A flight re-quoted at a different price on a later date is **not** an update —
it is a new observation. Because `as_of_date`, `price` and `duration` are all in
the key, the new quote appends and both rows survive. That is exactly what a
pricing analyst wants: `mart_booking_leadtime_pricing` can only show a fare
curve if yesterday's fare still exists.

**Type-2 SCD, and why it is not here.** A Type-2 dimension with
`valid_from`/`valid_to`/`is_current` would be the right pattern if the source
sent *corrections* to previously stated facts. It sends fresh snapshots instead,
and a date-partitioned snapshot fact already answers "what was the price on day
X" with a `where as_of_date = X`. Adding SCD columns would add machinery and no
new answers. If the source ever starts issuing corrections — same flight, same
date, restated price — that is the trigger to revisit, and the fact's
`delete+insert` on the key already handles the mechanics.

---

## Scenario 3: Duplicate file delivery

Two guards, cheapest first.

**File level.** Bronze computes `md5` of the file and checks
`(file_hash, as_of_date)` against `audit.ingestion_manifest`. A match means the
run exits having written nothing — a renamed re-send is caught, because the hash
is of contents, not the filename. `--force` overrides deliberately.

**Row level.** Even if a file slips past (say, one legitimately changed row
appended to yesterday's file, so the hash differs), Silver's business-key dedupe
catches the repeated rows and records them as `duplicate_business_key`.

Demonstrated:

```
[bronze] SKIP - file hash 1ebbe5cf5b96 already ingested for 2026-09-09.
         Use --force to override.
```

---

## Scenario 4: Pipeline reruns

Idempotency is enforced at every layer, not assumed:

| Layer | Mechanism | Effect of a rerun |
|---|---|---|
| Bronze | `mode("overwrite")` + `partitionOverwriteMode=dynamic` | Rewrites only the partitions in the current batch |
| Silver | Same dynamic overwrite, plus explicit cleanup of a rejected partition that now has zero rejects | Partition replaced, stale rejects cleared |
| dbt fact | `incremental_strategy='delete+insert'` on `flight_quote_sk` | Rows for the reprocessed snapshots are deleted, then reinserted |
| dbt marts | Full `table` rebuild from the fact | Deterministic; cheap because marts are aggregates |
| Redshift | `MERGE` on `flight_quote_sk` | Update-in-place, never append |

The stale-reject cleanup is worth naming because it is the easy one to miss: if
Monday's file had 40 rejects and Monday is reprocessed after an upstream fix,
dynamic overwrite writes no rows to a partition with nothing in it — so the old
40 rejects would linger forever, and the DQ dashboard would keep reporting a
problem that no longer exists. Silver removes the partition explicitly and
writes an empty one.

**Verified:** the day-2 run left `as_of_date=2026-09-09` at exactly 300,153 rows.

---

## Scenario 5: Late-arriving data

Two dates are needed and both are already kept, so no new mechanism is
required:

- `as_of_date`: the **logical** snapshot date the data describes.
- `_ingested_at` / `_batch_id` — when we actually received and processed it.

A file for the 8th arriving on the 11th is ingested with
`--as-of-date 2026-09-08`. It lands in its own logical partition; the fact's
`delete+insert` replaces that snapshot's rows; the marts rebuild and the numbers
for the 8th correct themselves. Nothing about the 9th, 10th or 11th is touched.

The one caveat, stated honestly: the fact's incremental filter is
`as_of_date >= max(as_of_date)`, so a very old late file would be filtered out
of an ordinary run. Two supported answers:

```bash
# targeted: rebuild the fact for one snapshot
dbt run -s fct_flight_price_quote --vars '{"start_date": "2026-09-08"}'

# blunt but always correct
dbt run --full-refresh -s fct_flight_price_quote+
```

In production the filter would use a watermark table keyed on
`_batch_id` rather than `max(as_of_date)`, which removes the caveat entirely.
That is a deliberate simplification for a 24-hour exercise, not an oversight.

---

## What a daily run costs

| Step | Work |
|---|---|
| Bronze | Read one file, write one partition |
| Silver | Read one partition, write two |
| dbt fact | `delete+insert` one snapshot |
| dbt marts | Rebuild aggregates (small; a full rebuild is cheaper than incremental bookkeeping at this size) |
| dbt tests | 72 tests |
| Redshift | `COPY` + `MERGE` + `ANALYZE` |

Nothing in that list scans the full history. The marts are the only full rebuild
and they are aggregates — at the point where that stops being cheap, they become
incremental on `as_of_date` with the same `delete+insert` pattern the fact
already uses.

---

## Orchestration

`pyspark/run_pipeline.py` chains Bronze then Silver in one process for
demonstration. In production each layer is its own task, because they fail
differently and deserve their own retries and SLAs:

```
file_sensor (S3 arrival, SLA 06:00)
    └─> bronze_ingest        (retry 2, alert on SchemaDriftError)
        └─> silver_transform (retry 2, alert on reconciliation failure)
            └─> dbt build    (retry 0 — a failing test must not be retried away)
                └─> redshift_load (COPY + MERGE + ANALYZE)
                    └─> dq_publish (refresh mart_data_quality_summary)
```

`dbt build` gets zero retries on purpose. Retrying a failed data-quality test
just delays the alert and risks loading data a test already flagged.
