# Assumptions

Every assumption below is a decision I made because the brief left it open. The
first one is load-bearing: most of the rest of the design follows from it.

---

## A1 — The source has no date column, so `as_of_date` is minted at ingestion

**The gap.** The file has 12 columns and none of them is a date.
`days_left` is a *relative* booking lead time (1–49 days). There is no departure
date, no quote date, no file date inside the data.

**The decision.** Bronze mints `as_of_date` at ingestion time, resolved in this
order: an explicit `--as-of-date` argument, then the `AS_OF_DATE` environment
variable, then the source file's modification time, then today. The entire CSV
is treated as **one price snapshot** taken on that date.

**Why.** Every requirement downstream needs a logical date to hang off:
partitioning, incremental loads, deduplication, reconciliation, the Redshift
sort key, and "did today's file arrive" monitoring. Inventing the date at the
boundary — once, explicitly, from metadata we control — is honest. The
alternative, deriving a departure date as `as_of_date + days_left`, would
manufacture a fact the source does not contain and would silently change
meaning if the ingestion date were ever wrong.

**Consequence.** `days_left` stays relative and is never converted into an
absolute date. A re-quote of the same flight on a later date appends a new row
rather than updating the old one, which is what gives the fact a price history.

---

## A2 — The business key includes `duration` and `price`

**The gap.** The brief asks how duplicates should be handled but does not define
what a duplicate *is*.

**The obvious answer was wrong.** The intuitive key —
`flight + source_city + destination_city + departure_time + arrival_time + class + days_left + as_of_date`
— collapses 300,153 rows to 235,761 distinct values. Using it as a dedupe key
would silently delete **64,392 rows (21.5% of the dataset)**.

I checked what those rows are before trusting the number. Of the 45,579
affected key groups, **zero contain identical rows** — every group differs in
`duration` and/or `price`. They are genuine separate fare and schedule variants
of the same flight number within the same time bucket: the source collapses
exact departure times into six buckets and lists each fare as its own row.
Across all 11 business attributes the file contains **zero exact duplicates**.

**The decision.** The business key is:

```
flight + source_city + destination_city + departure_time + arrival_time
       + class + days_left + duration + price + as_of_date
```

surrogated as `flight_quote_sk` (md5). This deduplicates genuinely repeated
delivery of the same row — the actual rerun and duplicate-file risk — while
preserving every real fare variant.

`quote_group_sk` (md5 of the narrower eight-column key) keeps the "same flight,
same lead time" grouping available as an analytical dimension, so widening the
key loses nothing. The count of multi-variant groups (45,579 on day one) is
logged per run as an *observation*, not a defect: a sudden change in it would
signal that the source's fare structure changed.

---

## A3 — Fare variants are a source characteristic, not a data-quality defect

Since the same flight legitimately appears with several fares in one snapshot,
the pipeline does not flag, average, or collapse them. Marts aggregate across
variants explicitly (`quote_count` is a count of *quotes*, not of flights) and
every mart states this in its grain. Reading `quote_count` as "number of
flights" would overstate capacity, which is why the column is not named
`flight_count`.

---

## A4 — Prices are INR and unconverted

Fares range from ₹1,105 to ₹123,071 on Indian domestic routes, so INR is the
obvious unit. Columns are named `price_inr` and `avg_price_inr` rather than
`price`, so no downstream consumer can mistake the unit. No FX conversion is
applied — that belongs in a currency dimension with dated rates, not hard-coded
in a transformation.

---

## A5 — Routes are directional

`Delhi → Mumbai` and `Mumbai → Delhi` are separate rows in `dim_route` with
separate `route_id` values. They are different commercial products with
different demand curves and different fares; collapsing them would average away
exactly the asymmetry a pricing team is looking for. `route_pair_name`
(`Delhi<->Mumbai`) is provided alongside for the cases where bidirectional
analysis is genuinely wanted.

---

## A6 — Rejected rows are retained, never dropped

Validation failures are written to `silver/flights_rejected/` with a
`rejection_reason` listing **every** rule the row broke, not just the first.
Nothing is deleted anywhere in the pipeline. Reconciliation
(`bronze = valid + rejected`) is asserted on every run and fails the job loudly
on mismatch.

On a clean day the rejected dataset is empty — so Silver still writes an empty,
correctly-typed partition rather than no partition at all, because a missing
file breaks every downstream contract that reads it. A rerun that fixes upstream
data also clears the rejects the previous run wrote.

---

## A7 — Business thresholds are declared, not discovered

The supplied file breaks none of the validation rules. Rules like
`price > 0`, `duration <= 60h`, `days_left <= 365` and
"a nonstop flight over 12 hours is implausible" are guard rails for *future*
files, set from the observed distribution plus domain sense (observed maximums:
₹123,071, 49.83 h, 49 days). They live in `pyspark/common/config.py::RULES` as
data, so the rules, their tests, and the documentation cannot drift apart.

Because a pipeline whose reject path never fires is untested,
`tests/make_dq_demo_file.py` builds a small file with one row per failure mode
and the run is documented in `documentation/data_quality.md`.

---

## A8 — Accepted value sets are a contract

The six airlines, six cities, six time-of-day buckets, three stop categories and
two cabin classes observed during profiling are locked in
`config.ACCEPTED_VALUES`. A new value is **rejected, not absorbed** — the
pipeline stops and a human decides whether a seventh airline is a genuine
network change or a data error. Standardization (trim, `initcap`, `lower`) runs
*before* validation, so cosmetic variation like `" economy "` is cleaned rather
than rejected; only genuinely new values fail.

---

## A9 — A missing or extra column fails the run; a reordered one does not

Column order changes are tolerated with a warning because every read selects by
name. A missing or unexpected column halts ingestion before any data is written:
that is a source-contract change and it needs a human decision, not a silent
schema widening.

---

## A10 — Local tooling stands in for cloud infrastructure

The brief does not require a live cluster. Bronze and Silver run on local
PySpark; dbt runs on DuckDB reading the Silver Parquet in place, which is close
enough to Redshift SQL that the models genuinely execute and the tests genuinely
pass. The `redshift` target in `profiles.yml` is the real shape of the
production connection, and the Spectrum DDL in
`redshift/ddl/05_external_spectrum.sql` is the swap-in path. No external dbt
packages are used, so the project builds offline from a clean clone.
