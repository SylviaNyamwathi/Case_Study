"""
Shared configuration for the Bronze and Silver PySpark jobs.

Single place for paths, the as_of_date decision, and the Spark session
factory so that Bronze and Silver cannot drift apart.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

# Repo root = two levels up from this file (pyspark/common/config.py)
REPO_ROOT = Path(__file__).resolve().parents[2]

RAW_DIR = REPO_ROOT / "data" / "raw"
LAKE_DIR = REPO_ROOT / "data" / "lake"

BRONZE_PATH = LAKE_DIR / "bronze" / "flights"
SILVER_VALID_PATH = LAKE_DIR / "silver" / "flights_valid"
SILVER_REJECTED_PATH = LAKE_DIR / "silver" / "flights_rejected"
MANIFEST_PATH = LAKE_DIR / "_manifest" / "ingested_files"
DQ_LOG_PATH = LAKE_DIR / "_dq" / "run_log"

DEFAULT_SOURCE_FILE = RAW_DIR / "airlines_flights_data.csv"


# ---------------------------------------------------------------------------
# ASSUMPTION (documented in README + documentation/assumptions.md)
# ---------------------------------------------------------------------------
# The source file carries no date column. Every downstream layer needs a
# logical date to partition, deduplicate and reconcile against, so we mint one
# at ingestion: as_of_date = the date the file arrived / was processed.
#
# The whole CSV is treated as ONE price snapshot taken on that date.
# `days_left` remains a relative booking lead time within that snapshot; it is
# deliberately NOT converted into an absolute departure date, because doing so
# would invent a fact the source does not contain.
# ---------------------------------------------------------------------------
def resolve_as_of_date(explicit: str | None = None, source_file: Path | None = None) -> date:
    """Resolve the logical snapshot date, in priority order.

    1. --as-of-date CLI argument (needed for backfills and reruns)
    2. AS_OF_DATE environment variable
    3. File modification time of the source file (file-arrival date)
    4. Today
    """
    raw = explicit or os.environ.get("AS_OF_DATE")
    if raw:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    if source_file and Path(source_file).exists():
        return datetime.fromtimestamp(Path(source_file).stat().st_mtime).date()
    return date.today()


# Accepted categorical value sets, locked from the profiling step
# (see documentation/silver_dq_findings.md). These are the contract: a value
# outside these sets is rejected, not silently passed through.
ACCEPTED_VALUES: dict[str, list[str]] = {
    "airline": ["AirAsia", "Air_India", "GO_FIRST", "Indigo", "SpiceJet", "Vistara"],
    "source_city": ["Bangalore", "Chennai", "Delhi", "Hyderabad", "Kolkata", "Mumbai"],
    "destination_city": ["Bangalore", "Chennai", "Delhi", "Hyderabad", "Kolkata", "Mumbai"],
    "departure_time": [
        "Afternoon",
        "Early_Morning",
        "Evening",
        "Late_Night",
        "Morning",
        "Night",
    ],
    "arrival_time": [
        "Afternoon",
        "Early_Morning",
        "Evening",
        "Late_Night",
        "Morning",
        "Night",
    ],
    "stops": ["zero", "one", "two_or_more"],
    "class": ["Business", "Economy"],
}

# Business rule thresholds, kept as data so tests and docs reference one source.
RULES = {
    "price_min": 1,          # price must be a positive fare
    "price_max": 500_000,    # sanity ceiling (observed max ~123,071 INR)
    "duration_min": 0.1,     # hours
    "duration_max": 60.0,    # observed max ~49.83h; above this is implausible
    "days_left_min": 0,
    "days_left_max": 365,
    # A zero-stop flight longer than this many hours is implausible for the
    # Indian domestic network and is flagged rather than trusted.
    "nonstop_max_duration": 12.0,
}


@dataclass(frozen=True)
class JobContext:
    """Everything a job needs to know about the run it is executing."""

    as_of_date: date
    batch_id: str
    source_file: Path

    @property
    def as_of_date_str(self) -> str:
        return self.as_of_date.isoformat()


def make_batch_id(as_of: date) -> str:
    """Deterministic-per-run batch id: <as_of_date>T<utc timestamp>."""
    return f"{as_of.isoformat()}T{datetime.utcnow().strftime('%H%M%S')}"


def get_spark(app_name: str):
    """Local Spark session tuned for a laptop-sized run.

    Kept small on purpose: the same code runs on a cluster by removing
    .master("local[*]") and letting spark-submit supply the master.
    """
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", os.environ.get("SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.driver.memory", os.environ.get("DRIVER_MEMORY", "2g"))
        .getOrCreate()
    )
