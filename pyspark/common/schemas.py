"""
Schema contracts.

Everything is declared explicitly. `inferSchema=True` is never used, for three
reasons worth saying out loud in a review:

1. Inference costs an extra full pass over the file.
2. Inference is *data dependent* - tomorrow's file can infer a different type
   for the same column (e.g. an all-integer `duration` column arriving as
   IntegerType), which silently changes downstream behaviour.
3. A declared schema turns schema drift into a loud, catchable event instead of
   an invisible one. That is the whole point of `assert_no_schema_drift`.
"""

from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# --- Source contract -------------------------------------------------------
# Exactly the 12 columns of airlines_flights_data.csv, in file order.
# `index` is the source row number; kept in Bronze for lineage, dropped later.
SOURCE_SCHEMA = StructType(
    [
        StructField("index", IntegerType(), True),
        StructField("airline", StringType(), True),
        StructField("flight", StringType(), True),
        StructField("source_city", StringType(), True),
        StructField("departure_time", StringType(), True),
        StructField("stops", StringType(), True),
        StructField("arrival_time", StringType(), True),
        StructField("destination_city", StringType(), True),
        StructField("class", StringType(), True),
        StructField("duration", DoubleType(), True),
        StructField("days_left", IntegerType(), True),
        StructField("price", IntegerType(), True),
    ]
)

SOURCE_COLUMNS = [f.name for f in SOURCE_SCHEMA.fields]

# Metadata columns added by Bronze. Named with a leading underscore so they are
# visibly *ours*, never mistaken for source-supplied fields.
BRONZE_METADATA_COLUMNS = [
    "_source_file",     # which file this row came from
    "_source_row",      # source `index` preserved for lineage
    "_ingested_at",     # wall-clock ingestion timestamp (UTC)
    "_batch_id",        # one id per pipeline execution
    "_file_hash",       # md5 of the source file - duplicate-delivery detection
    "as_of_date",       # logical snapshot date == Bronze partition key
]


class SchemaDriftError(Exception):
    """Raised when an incoming file does not match the declared contract."""


def read_header(path: str) -> list[str]:
    """Read just the CSV header line, without loading the file into Spark."""
    with open(path, "r", encoding="utf-8-sig") as fh:
        return [c.strip() for c in fh.readline().strip().split(",")]


def detect_schema_drift(header: list[str]) -> dict[str, list[str]]:
    """Compare an incoming header against SOURCE_COLUMNS.

    Returns a dict describing the drift. Empty dict == no drift.
    Ordering changes are reported separately from added/removed columns,
    because a reorder is safe (we select by name) while a missing column is not.
    """
    expected = set(SOURCE_COLUMNS)
    actual = set(header)
    drift: dict[str, list[str]] = {}

    missing = sorted(expected - actual)
    unexpected = sorted(actual - expected)
    if missing:
        drift["missing_columns"] = missing
    if unexpected:
        drift["unexpected_columns"] = unexpected
    if not missing and not unexpected and header != SOURCE_COLUMNS:
        drift["reordered_columns"] = header

    return drift


def assert_no_schema_drift(path: str, allow_reorder: bool = True) -> dict[str, list[str]]:
    """Fail the run loudly on structural drift; tolerate a pure reorder.

    Policy (documented in documentation/data_quality.md):
      - missing column    -> FAIL the run. Bronze cannot be trusted.
      - unexpected column -> FAIL the run. Someone changed the source contract;
                             a human decides whether to widen the schema.
      - reordered only    -> WARN and continue, because we select by name.
    """
    drift = detect_schema_drift(read_header(path))
    if not drift:
        return {}

    if allow_reorder and set(drift) == {"reordered_columns"}:
        print(f"[WARN] schema drift (column order changed, tolerated): {drift}")
        return drift

    raise SchemaDriftError(
        f"Schema drift detected in {path}: {drift}. "
        "Bronze ingestion halted - update SOURCE_SCHEMA deliberately, "
        "or quarantine the file."
    )
