# Silver Profiling & Data Quality Findings

Profiled with a throwaway notebook and DuckDB over the Bronze partition
(`as_of_date=2026-09-09`). Every number below is measured, not estimated.

## Headline shape

| Property | Value |
|---|---|
| Rows | 300,153 |
| Columns | 12 |
| Nulls | **0** across all 12 columns |
| Exact duplicate rows (all 11 business attributes) | **0** |
| Distinct airlines | 6 |
| Distinct flight numbers | 1,561 |
| Cities (origin and destination) | 6 each → 30 directional routes |
| Time-of-day buckets | 6 (departure and arrival) |
| Stop categories | 3 |
| Cabin classes | 2 |
| Price range | ₹1,105 – ₹123,071 |
| Duration range | 0.83 h – 49.83 h |
| `days_left` range | 1 – 49 |

Volume by carrier is heavily skewed, Vistara 127,859 rows, SpiceJet 9,011
which matters for the cabin-mix mart: share must be computed per route, not
network-wide, or Vistara dominates every chart by construction.

| Cabin | Rows | Avg fare |
|---|---|---|
| Economy | 206,666 | ₹6,572 |
| Business | 93,487 | ₹52,540 |

Business averages roughly **8×** Economy. That ratio is the sanity anchor for
`business_premium_pct` in Gold mart 3, and the reason
`assert_business_premium_is_sane.sql` fails the build on a negative premium.

---

## Finding 1: The intuitive business key would delete 21.5% of the data

This is the finding that changed the design.

| Key | Distinct values | Rows it would drop |
|---|---|---|
| `flight + route + time buckets + class + days_left + as_of_date` (8 cols) | 235,761 | **64,392 (21.5%)** |
| … + `duration` | 298,478 | 1,675 |
| … + `duration + price` (10 cols, **chosen**) | **300,153** | **0** |

Before trusting the 21.5%, I inspected the 45,579 affected groups:

- groups where all rows are identical: **0**
- groups where `price` differs: 19,088
- groups where `duration` differs: 44,996

So none of them are duplicates. They are separate fare and schedule variants of
the same flight number inside the same time bucket — the source collapses exact
departure times into six buckets and lists each fare as its own row.
Distribution of variants per group:

| Variants in group | Groups |
|---|---|
| 1 | 190,182 |
| 2 | 32,783 |
| 3 | 9,167 |
| 4 | 2,301 |
| 5 | 840 |
| 6–12 | 488 |

**Action.** `duration` and `price` joined the business key (see
`documentation/assumptions.md` A2). `quote_group_sk` preserves the narrow
grouping as a dimension, and `multi_variant_quote_groups` (45,579) is logged
per run so a change in the source's fare structure is visible immediately.

**Locked into a test.** `tests/test_silver_rules.py::test_fare_variants_get_different_keys_but_the_same_group`
fails if anyone narrows the key again.

---

## Finding 2: `duration` vs `stops` is coherent, so the rule is preventive

The guide flagged a possible "49.83 h zero-stop flight". Checked directly:

| Stops | Rows | Min duration | Max duration |
|---|---|---|---|
| zero | 36,004 | 0.83 h | **3.58 h** |
| one | 250,863 | 2.92 h | 49.83 h |
| two_or_more | 13,286 | 3.92 h | 49.83 h |

Nonstop flights top out at 3.58 h, entirely plausible for Indian domestic
routes, and **zero** nonstop flights exceed 12 hours. The
`implausible_nonstop_duration` rule (nonstop > 12 h) therefore fires on nothing
today. It stays in as a guard rail for future files, and is proven to work
against the injected demo row rather than against real data.

The 49.83 h maxima on one-stop and two-stop itineraries are long layovers, not
errors, so they pass. The absolute ceiling is set at 60 h.

---

## Finding 3: Zero rejections on the supplied file

Running all 16 rules over the real file:

```
bronze_row_count:           300,153
silver_valid_count:         300,153
silver_rejected_count:            0
duplicates_removed:               0
multi_variant_quote_groups:  45,579
rejected_pct:                   0.0
reconciled:                    true
```

The file is genuinely clean. That is a problem for a review: an untriggered
reject path is an untested reject path. So
`tests/make_dq_demo_file.py` builds a 2,009-row file with nine deliberately
broken rows, and the documented demo run produces:

```
as_of_date 2026-09-10
bronze_row_count:  2,009
silver_valid:      2,000
silver_rejected:       9
rejected_pct:      0.448
reconciled:         true

price_out_of_range              2
duration_out_of_range           1
days_left_out_of_range          1
same_source_and_destination     1
invalid_airline                 1
invalid_class                   1
invalid_stops                   1
implausible_nonstop_duration    1
```

Nine rows, eight distinct reasons, `price_out_of_range` catches both a zero
and a negative fare. Full detail in `documentation/data_quality.md`.

---

## Finding 4: Accepted value sets, locked as a contract

Baseline captured from this file and enforced from now on
(`pyspark/common/config.py::ACCEPTED_VALUES`):

| Column | Accepted values |
|---|---|
| `airline` | AirAsia, Air_India, GO_FIRST, Indigo, SpiceJet, Vistara |
| `source_city`, `destination_city` | Bangalore, Chennai, Delhi, Hyderabad, Kolkata, Mumbai |
| `departure_time`, `arrival_time` | Afternoon, Early_Morning, Evening, Late_Night, Morning, Night |
| `stops` | zero, one, two_or_more |
| `class` | Business, Economy |

A seventh airline or a new city is **rejected, not absorbed**. The same sets are
re-asserted as `accepted_values` tests in the dbt staging layer, so drift is
caught twice, once in PySpark on ingest, once in dbt before it reaches a mart.

---

## Finding 5: Standardization must run before validation

`class` arriving as `" economy "` is a cosmetic problem, not a data-quality
failure. Silver trims and normalizes casing first, then validates, so cosmetic
variation is cleaned while genuinely unknown values still fail. This ordering is
asserted by
`tests/test_silver_rules.py::test_whitespace_and_casing_are_standardized_not_rejected`.

---

## Finding 6: `index` is source lineage, not a key

The source `index` column (0–300,152) is a row number from the export, not a
business identifier. It is renamed `_source_row` and kept in Bronze and Silver
for lineage, and it is the deterministic dedupe tiebreak, but it never becomes a
key: a second file would restart its numbering at 0 and collide with the first.
