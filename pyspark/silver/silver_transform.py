"""
SILVER - trusted data.

What happens here, in order:
  1. Standardize: trim strings, normalize casing of categoricals, derive
     `stops_count` from the text `stops` column, derive `route`.
  2. Validate: every rule from common/config.RULES plus accepted-value sets.
     Failures are ACCUMULATED (not short-circuited) so a row's
     `rejection_reason` lists every reason it failed, not just the first.
  3. Deduplicate on the business key, tiebreak = latest _ingested_at, then
     lowest _source_row. Deterministic, and documented.
  4. Split output: silver_valid/ and silver_rejected/ (with rejection_reason).
     Nothing is dropped - a rejected row is still queryable and explainable.
  5. Reconcile: bronze_count == valid + rejected + duplicates_removed.

Business key (identical string in README and dbt) - see the long comment on
BUSINESS_KEY below for why duration and price are part of it:
    flight + source_city + destination_city + departure_time + arrival_time
    + class + days_left + duration + price + as_of_date

Run:
    python pyspark/silver/silver_transform.py
    python pyspark/silver/silver_transform.py --as-of-date 2026-09-08
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common import audit, config  # noqa: E402

# ---------------------------------------------------------------------------
# BUSINESS KEY - and why it is not the obvious one
# ---------------------------------------------------------------------------
# The intuitive key is flight + route + time buckets + class + days_left +
# as_of_date. Profiling killed that idea: it collapses 300,153 source rows to
# 235,761 distinct keys, i.e. it would silently DELETE 64,392 rows (21.5%).
#
# Checking what those "duplicates" actually are: of 45,579 affected key groups,
# ZERO are identical rows - every one differs in `duration` and/or `price`.
# They are separate fare/schedule variants of the same flight number in the same
# lead-time bucket (the source collapses exact departure times into 6 buckets
# and lists each fare separately). Dropping them is data loss, not deduplication.
#
# Across all 11 source business attributes there are ZERO exact duplicate rows.
# So the key includes duration and price: it deduplicates genuine repeated
# delivery of the same row (the actual rerun / duplicate-file risk) while
# preserving every real fare variant.
#
# `quote_group_sk` below keeps the narrower grouping available as an analytical
# dimension, so nothing is lost by widening the key.
# ---------------------------------------------------------------------------
BUSINESS_KEY = [
    "flight",
    "source_city",
    "destination_city",
    "departure_time",
    "arrival_time",
    "class",
    "days_left",
    "duration",
    "price",
    "as_of_date",
]

# Narrower "same flight, same lead time" grouping - NOT a uniqueness key.
QUOTE_GROUP_KEY = [
    "flight",
    "source_city",
    "destination_city",
    "departure_time",
    "arrival_time",
    "class",
    "days_left",
    "as_of_date",
]

STRING_COLUMNS = [
    "airline",
    "flight",
    "source_city",
    "departure_time",
    "stops",
    "arrival_time",
    "destination_city",
    "class",
]

STOPS_MAP = {"zero": 0, "one": 1, "two_or_more": 2}


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Silver transform for airline flight pricing")
    p.add_argument(
        "--as-of-date",
        default=None,
        help="Process one snapshot only (YYYY-MM-DD). Omit to process all Bronze partitions.",
    )
    return p.parse_args(argv)


def standardize(df):
    """Cheap, reversible cleaning only. Nothing here rejects a row."""
    out = df
    for col in STRING_COLUMNS:
        out = out.withColumn(col, F.trim(F.col(col)))

    # Categorical casing: source is consistent today, but normalizing means a
    # future 'ECONOMY' or ' economy ' lands as 'Economy' instead of being
    # rejected as an invalid category.
    out = (
        out.withColumn("class", F.initcap(F.col("class")))
        .withColumn("stops", F.lower(F.col("stops")))
        .withColumn("flight", F.upper(F.col("flight")))
    )

    # Derived, analysis-friendly columns.
    stops_expr = F.create_map([F.lit(x) for kv in STOPS_MAP.items() for x in kv])
    out = (
        out.withColumn("stops_count", stops_expr[F.col("stops")].cast("int"))
        .withColumn("is_nonstop", F.col("stops") == F.lit("zero"))
        .withColumn(
            "route", F.concat_ws("-", F.col("source_city"), F.col("destination_city"))
        )
        .withColumn("duration_minutes", F.round(F.col("duration") * 60).cast("int"))
        .withColumn(
            "price_inr", F.col("price").cast("decimal(12,2)")
        )
        .withColumn(
            "flight_quote_sk",
            F.md5(F.concat_ws("||", *[F.col(c).cast("string") for c in BUSINESS_KEY])),
        )
        .withColumn(
            "quote_group_sk",
            F.md5(F.concat_ws("||", *[F.col(c).cast("string") for c in QUOTE_GROUP_KEY])),
        )
    )
    return out


def validation_rules() -> list[tuple[str, "F.Column"]]:
    """(rejection_reason, condition_that_means_INVALID) pairs.

    Kept as data so tests/test_silver_rules.py can iterate the same list the
    job uses - the rules cannot drift away from their tests.
    """
    r = config.RULES
    rules: list[tuple[str, "F.Column"]] = [
        # --- completeness (required fields) ---
        ("null_flight", F.col("flight").isNull() | (F.col("flight") == "")),
        ("null_airline", F.col("airline").isNull() | (F.col("airline") == "")),
        ("null_price", F.col("price").isNull()),
        ("null_duration", F.col("duration").isNull()),
        ("null_days_left", F.col("days_left").isNull()),
        ("null_route_city", F.col("source_city").isNull() | F.col("destination_city").isNull()),
        # --- business validity ---
        ("price_out_of_range", (F.col("price") < r["price_min"]) | (F.col("price") > r["price_max"])),
        (
            "duration_out_of_range",
            (F.col("duration") < r["duration_min"]) | (F.col("duration") > r["duration_max"]),
        ),
        (
            "days_left_out_of_range",
            (F.col("days_left") < r["days_left_min"]) | (F.col("days_left") > r["days_left_max"]),
        ),
        ("same_source_and_destination", F.col("source_city") == F.col("destination_city")),
        (
            "implausible_nonstop_duration",
            (F.col("stops") == F.lit("zero")) & (F.col("duration") > r["nonstop_max_duration"]),
        ),
    ]
    # --- accepted values (one rule per categorical, so the reason names the column) ---
    for col, allowed in config.ACCEPTED_VALUES.items():
        rules.append((f"invalid_{col}", ~F.col(col).isin(allowed)))
    return rules


def apply_validation(df):
    """Attach `rejection_reasons` (array) and `is_valid` (bool)."""
    reason_cols = [
        F.when(cond, F.lit(name)).otherwise(F.lit(None)) for name, cond in validation_rules()
    ]
    out = df.withColumn("rejection_reasons", F.array_compact(F.array(*reason_cols)))
    return out.withColumn("is_valid", F.size("rejection_reasons") == 0)


def deduplicate(df):
    """Dedupe on the business key.

    Tiebreak, documented: keep the row with the latest `_ingested_at`; if two
    rows tie there (same file), keep the lowest `_source_row`. This is
    deterministic, which matters for reruns - the same input always yields the
    same surviving row.
    """
    win = Window.partitionBy(*BUSINESS_KEY).orderBy(
        F.col("_ingested_at").desc(), F.col("_source_row").asc()
    )
    ranked = df.withColumn("_dedupe_rank", F.row_number().over(win))
    kept = ranked.where(F.col("_dedupe_rank") == 1).drop("_dedupe_rank")
    dropped = ranked.where(F.col("_dedupe_rank") > 1).drop("_dedupe_rank")
    return kept, dropped


def main(argv=None) -> int:
    args = parse_args(argv)
    spark = config.get_spark("silver_transform")

    bronze = spark.read.parquet(str(config.BRONZE_PATH))
    if args.as_of_date:
        bronze = bronze.where(F.col("as_of_date") == F.lit(args.as_of_date))
        print(f"[silver] processing snapshot {args.as_of_date} only")

    bronze_count = bronze.count()
    if bronze_count == 0:
        raise SystemExit("[silver] no Bronze rows to process - run bronze_ingest first")
    print(f"[silver] bronze rows in scope: {bronze_count:,}")

    standardized = standardize(bronze)
    validated = apply_validation(standardized)

    # Dedupe AFTER validation so a rejected duplicate is still explainable,
    # and BEFORE the split so the valid set is unique on the business key.
    deduped, duplicates = deduplicate(validated)
    duplicate_count = duplicates.count()

    valid = deduped.where(F.col("is_valid")).drop("rejection_reasons", "is_valid")
    rejected = (
        deduped.where(~F.col("is_valid"))
        .withColumn("rejection_reason", F.concat_ws(",", F.col("rejection_reasons")))
        .drop("is_valid")
    )
    rejected_dupes = duplicates.withColumn(
        "rejection_reason", F.lit("duplicate_business_key")
    ).drop("is_valid")
    rejected_all = rejected.unionByName(rejected_dupes, allowMissingColumns=True)

    valid_count = valid.count()
    rejected_count = rejected_all.count()

    valid_cols = [
        "flight_quote_sk",
        "quote_group_sk",
        "as_of_date",
        "airline",
        "flight",
        "source_city",
        "destination_city",
        "route",
        "departure_time",
        "arrival_time",
        "stops",
        "stops_count",
        "is_nonstop",
        "class",
        "duration",
        "duration_minutes",
        "days_left",
        "price_inr",
        "_source_file",
        "_source_row",
        "_ingested_at",
        "_batch_id",
    ]

    (
        valid.select(*valid_cols)
        .write.mode("overwrite")
        .partitionBy("as_of_date")
        .parquet(str(config.SILVER_VALID_PATH))
    )
    rejected_out = rejected_all.select(*valid_cols, "rejection_reason")
    (
        rejected_out.write.mode("overwrite")
        .partitionBy("as_of_date")
        .parquet(str(config.SILVER_REJECTED_PATH))
    )

    # A clean day produces no rejected rows, and Spark then writes no partition
    # at all - which breaks every downstream contract that reads the rejected
    # dataset. So materialize an empty, correctly-typed partition per snapshot
    # instead: the DQ models keep working, and "zero rejects" stays a readable
    # fact rather than a missing file.
    dates_with_rejects = {
        r["as_of_date"] for r in rejected_out.select("as_of_date").distinct().collect()
    }
    for row in bronze.select("as_of_date").distinct().collect():
        as_of = row["as_of_date"]
        if as_of in dates_with_rejects:
            continue
        partition_dir = config.SILVER_REJECTED_PATH / f"as_of_date={as_of}"
        # rmtree first: a rerun that FIXES upstream data must clear the rejects
        # that the previous run wrote, otherwise stale rejections linger forever
        # (dynamic partition overwrite cannot clear a partition it writes no
        # rows to).
        if partition_dir.exists():
            shutil.rmtree(partition_dir)
        (
            rejected_out.drop("as_of_date")
            .limit(0)
            .write.mode("overwrite")
            .parquet(str(partition_dir))
        )
        print(f"[silver] no rejects for {as_of} - wrote empty partition")

    # --- DQ control 5: source-to-target reconciliation ---------------------
    reconciled = bronze_count == (valid_count + rejected_count)
    rejected_pct = round(100.0 * rejected_count / bronze_count, 4) if bronze_count else 0.0

    # Observation, not a defect: how many (flight, route, class, lead time)
    # groups carry more than one fare/duration variant. Tracked so a sudden jump
    # or collapse in this number is visible - it would signal a source change.
    multi_variant_groups = (
        valid.groupBy("quote_group_sk")
        .agg(F.count("*").alias("variants"))
        .where(F.col("variants") > 1)
        .count()
    )

    reason_breakdown = {
        row["rejection_reason"]: row["n"]
        for row in rejected_all.groupBy("rejection_reason")
        .agg(F.count("*").alias("n"))
        .orderBy(F.desc("n"))
        .limit(25)
        .collect()
    }

    audit.log_dq_run(
        spark,
        {
            "layer": "silver",
            "as_of_date": args.as_of_date or "all",
            "bronze_row_count": bronze_count,
            "silver_valid_count": valid_count,
            "silver_rejected_count": rejected_count,
            "duplicates_removed": duplicate_count,
            "multi_variant_quote_groups": multi_variant_groups,
            "rejected_pct": rejected_pct,
            "reconciled": reconciled,
            "rejection_reason_breakdown": reason_breakdown,
        },
    )

    if not reconciled:
        spark.stop()
        raise SystemExit(
            f"[silver] RECONCILIATION FAILED: bronze={bronze_count} != "
            f"valid={valid_count} + rejected={rejected_count}"
        )

    print(f"[silver] valid    {valid_count:,} -> {config.SILVER_VALID_PATH}")
    print(f"[silver] rejected {rejected_count:,} -> {config.SILVER_REJECTED_PATH}")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
