# Data Quality & Reconciliation

The brief (section 9) asks for five specific controls. Each maps to a concrete
mechanism in the code, and each is demonstrated by a run below.

| Requirement | Where it lives | Fails how |
|---|---|---|
| Missing or invalid values | 16 rules in `silver_transform.validation_rules()` | Row → `silver_rejected/` with reason; run continues |
| Duplicate records | Business-key dedupe with documented tiebreak | Row → `silver_rejected/` as `duplicate_business_key` |
| Invalid business values | `config.ACCEPTED_VALUES` + `RULES`, re-asserted as dbt `accepted_values` | Row rejected in Silver; dbt test fails the build if one slips through |
| Unexpected schema changes | `schemas.assert_no_schema_drift()`, before any read | **Halts ingestion**, nothing is written |
| Source-to-target discrepancies | Reconciliation in Silver + two dbt singular tests + a Redshift query | **Fails the run** |

---

## 1. Validation rules

All 16 rules are declared as data in `pyspark/common/config.py::RULES` and
`ACCEPTED_VALUES`, and evaluated in `silver_transform.validation_rules()`. Two
design choices worth naming:

**Reasons accumulate.** A row that breaks three rules reports all three, not
just the first. Short-circuiting means a data team fixes one problem, reruns,
discovers the next, and iterates blind.

**Standardize, then validate.** Trim and casing normalization run first, so
`" economy "` is cleaned rather than rejected. Only genuinely unknown values
fail.

| Rule | Condition that rejects |
|---|---|
| `null_flight`, `null_airline`, `null_price`, `null_duration`, `null_days_left`, `null_route_city` | Required field null or empty |
| `price_out_of_range` | `price < 1` or `price > 500,000` |
| `duration_out_of_range` | `duration < 0.1 h` or `> 60 h` |
| `days_left_out_of_range` | `days_left < 0` or `> 365` |
| `same_source_and_destination` | Origin equals destination |
| `implausible_nonstop_duration` | `stops = zero` and `duration > 12 h` |
| `invalid_airline`, `invalid_source_city`, `invalid_destination_city`, `invalid_departure_time`, `invalid_arrival_time`, `invalid_stops`, `invalid_class` | Value outside the accepted set |

Thresholds are guard rails for future files, not reactions to this one, the
supplied file breaks none of them. Rationale for each boundary is in
`documentation/silver_dq_findings.md`.

---

## 2. Duplicate handling

Dedupe key (10 columns, see `assumptions.md` A2 for why `duration` and `price`
are in it):

```
flight + source_city + destination_city + departure_time + arrival_time
       + class + days_left + duration + price + as_of_date
```

Tiebreak, in order: latest `_ingested_at`, then lowest `_source_row`.
Deterministic on purpose — the same input always keeps the same row, so reruns
are reproducible. Losers are written to `silver_rejected/` as
`duplicate_business_key` rather than dropped, and
`test_dedupe_tiebreak_is_deterministic` asserts it.

Duplicate *files* are caught earlier and more cheaply: Bronze hashes the file
and checks `(file_hash, as_of_date)` against the ingestion manifest. A re-sent
file, even renamed is a no-op that writes zero rows. `--force` overrides it
deliberately.

---

## 3. Schema drift

`assert_no_schema_drift()` reads only the header line and compares it to the
declared `SOURCE_SCHEMA` **before** Spark opens the file.

| Drift | Policy | Why |
|---|---|---|
| Missing column | **Fail the run** | Bronze cannot be trusted; a human decides |
| Unexpected column | **Fail the run** | The source contract changed; widening the schema is a deliberate act |
| Reordered only | Warn and continue | Every read selects by name, so order is harmless |

Failing before the write is the point: a partial Bronze partition is worse than
no partition. Seven tests in `tests/test_schema_drift.py` cover each case,
including that the declared schema still matches the supplied file.

---

## 4. Reconciliation

Asserted at three levels, because a count that agrees with itself in one place
proves nothing:

**In Silver (PySpark).** `bronze_count == valid_count + rejected_count`,
computed every run and written to the DQ log. Mismatch raises and exits
non-zero.

**In dbt.** `assert_fact_reconciles_to_staging.sql` (every valid Silver row
appears exactly once in the fact) and
`assert_route_performance_totals_match_fact.sql` (`sum(quote_count)` in the mart
equals `count(*)` in the fact, per snapshot). An aggregation that quietly loses
or duplicates rows fails the build.

**In Redshift.** The manifest's `source_row_count` versus the loaded fact count,
in `redshift/ddl/05_external_spectrum.sql`.

---

## 5. Proof: the two documented runs

### Run 1: the supplied file (clean)

```
python pyspark/run_pipeline.py
```

(the supplied CSV must be at `data/raw/airlines_flights_data.csv` - it is not
committed to the repository)

```
layer:                      bronze → silver
bronze_row_count:           300,153
silver_valid_count:         300,153
silver_rejected_count:            0
duplicates_removed:               0
multi_variant_quote_groups:  45,579
rejected_pct:                   0.0
reconciled:                    true
```

### Run 2: the injected-fault file (proves the reject path)

```
python tests/make_dq_demo_file.py
python pyspark/run_pipeline.py \
    --source-file data/raw/samples/flights_dq_demo.csv \
    --as-of-date 2026-09-10
```

```
bronze_row_count:   2,009
silver_valid_count: 2,000
silver_rejected:        9
rejected_pct:       0.448
reconciled:          true

rejection_reason_breakdown:
  price_out_of_range              2   (a zero fare and a negative fare)
  duration_out_of_range           1
  days_left_out_of_range          1
  same_source_and_destination     1
  invalid_airline                 1
  invalid_class                   1
  invalid_stops                   1
  implausible_nonstop_duration    1
```

Day one's partition is untouched by run 2, dynamic partition overwrite replaces
only `as_of_date=2026-09-10`. Both snapshots are queryable side by side:

```
as_of_date   valid rows
2026-09-09      300,153
2026-09-10        2,000
```

The rejected rows are inspectable with their reasons:

```sql
select as_of_date, rejection_reason, count(*)
from read_parquet('data/lake/silver/flights_rejected/**/*.parquet',
                  hive_partitioning = 1)
group by 1, 2 order by 1, 3 desc;
```

---

## 6. Automated test coverage

| Suite | Count | Result |
|---|---|---|
| `pytest tests`: Silver rules, standardization, keys, dedupe, schema drift | 32 | **32 passed** |
| `dbt build`: models + generic tests + singular tests | 72 | **PASS=72, ERROR=0** |

dbt tests break down as: `not_null` and `unique` on every key, `accepted_values`
on every categorical, `relationships` from fact to both dimensions,
`unique_combination` on each mart's stated grain, `accepted_range` on
`competing_airline_count`, plus five singular tests,
`assert_price_is_positive`, `assert_fact_reconciles_to_staging`,
`assert_route_performance_totals_match_fact`, `assert_no_self_routes`,
`assert_business_premium_is_sane`.

`accepted_range` and `unique_combination` are hand-written in
`dbt/kenya_airways/tests/generic/generic_tests.sql` rather than pulled from
`dbt_utils`, so the project builds with no package download,
`packages.yml.example` shows the package-based alternative.

---

## 7. What is deliberately not automated

- **Fare reasonableness.** A ₹123,071 Business fare is high but real. Rejecting
  statistical outliers would delete genuine premium fares; the marts surface
  spread and standard deviation so an analyst can judge instead.
- **Cross-carrier duration comparison.** Two carriers reporting different
  durations for the same route is normal (different aircraft, different
  routings), not an error.
- **Referential integrity in Redshift.** Keys are declared for the planner but
  not enforced. Enforcement lives in dbt tests, where a failure is a readable
  message rather than a load abort at 3 a.m.
