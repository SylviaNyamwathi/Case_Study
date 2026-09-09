"""
Unit tests for the Silver validation and standardization rules.

These run on a handful of hand-built rows through a local SparkSession, so they
finish in seconds and can sit in CI. They deliberately import the SAME rule
list the job uses (silver_transform.validation_rules) rather than restating the
rules - a rule that changes in the job cannot pass a stale test here.

    pytest tests -v
"""

from __future__ import annotations

import sys
from datetime import date, datetime
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "pyspark"))

from common import config  # noqa: E402
from silver import silver_transform as st  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    s = config.get_spark("pytest_silver_rules")
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


BRONZE_COLUMNS = [
    "_source_row",
    "airline",
    "flight",
    "source_city",
    "departure_time",
    "stops",
    "arrival_time",
    "destination_city",
    "class",
    "duration",
    "days_left",
    "price",
    "_source_file",
    "_ingested_at",
    "_batch_id",
    "_file_hash",
    "as_of_date",
]


def bronze_row(**overrides):
    """A valid Bronze row; override one field to make it invalid."""
    row = {
        "_source_row": 1,
        "airline": "Vistara",
        "flight": "UK-995",
        "source_city": "Delhi",
        "departure_time": "Morning",
        "stops": "zero",
        "arrival_time": "Afternoon",
        "destination_city": "Mumbai",
        "class": "Economy",
        "duration": 2.25,
        "days_left": 5,
        "price": 5955,
        "_source_file": "test.csv",
        "_ingested_at": datetime(2026, 9, 9, 12, 0, 0),
        "_batch_id": "test-batch",
        "_file_hash": "deadbeef",
        "as_of_date": date(2026, 9, 9),
    }
    row.update(overrides)
    return tuple(row[c] for c in BRONZE_COLUMNS)


def build(spark, rows):
    schema = (
        "_source_row int, airline string, flight string, source_city string, "
        "departure_time string, stops string, arrival_time string, "
        "destination_city string, class string, duration double, days_left int, "
        "price int, _source_file string, _ingested_at timestamp, _batch_id string, "
        "_file_hash string, as_of_date date"
    )
    df = spark.createDataFrame(rows, schema=schema)
    return st.apply_validation(st.standardize(df))


def reasons_for(spark, **overrides) -> list[str]:
    result = build(spark, [bronze_row(**overrides)]).collect()[0]
    return sorted(result["rejection_reasons"])


# --------------------------------------------------------------------- valid
def test_a_clean_row_is_valid(spark):
    assert reasons_for(spark) == []


def test_standardization_derives_stops_and_route(spark):
    row = build(spark, [bronze_row(stops="one")]).collect()[0]
    assert row["stops_count"] == 1
    assert row["is_nonstop"] is False
    assert row["route"] == "Delhi-Mumbai"
    assert row["duration_minutes"] == 135


def test_whitespace_and_casing_are_standardized_not_rejected(spark):
    # ' economy ' would fail the accepted-values check if we did not clean it
    # first. This asserts the order of operations: standardize, THEN validate.
    assert reasons_for(spark, **{"class": "  economy  "}) == []


def test_surrogate_keys_are_deterministic(spark):
    a = build(spark, [bronze_row()]).collect()[0]
    b = build(spark, [bronze_row(_source_row=99, _batch_id="other")]).collect()[0]
    # Same business attributes, different lineage metadata -> same key.
    assert a["flight_quote_sk"] == b["flight_quote_sk"]
    assert a["quote_group_sk"] == b["quote_group_sk"]


def test_fare_variants_get_different_keys_but_the_same_group(spark):
    # This is the 21.5% finding, locked into a test: two fare variants of the
    # same flight in the same lead-time bucket must NOT collide.
    a = build(spark, [bronze_row(price=5955)]).collect()[0]
    b = build(spark, [bronze_row(price=7999, duration=2.5)]).collect()[0]
    assert a["flight_quote_sk"] != b["flight_quote_sk"]
    assert a["quote_group_sk"] == b["quote_group_sk"]


# ------------------------------------------------------------------ rejected
@pytest.mark.parametrize(
    "overrides,expected_reason",
    [
        ({"price": 0}, "price_out_of_range"),
        ({"price": -100}, "price_out_of_range"),
        ({"price": 9_000_000}, "price_out_of_range"),
        ({"duration": 0.0}, "duration_out_of_range"),
        ({"duration": 99.0}, "duration_out_of_range"),
        ({"days_left": -1}, "days_left_out_of_range"),
        ({"days_left": 400}, "days_left_out_of_range"),
        ({"destination_city": "Delhi"}, "same_source_and_destination"),
        ({"airline": "KenyaAirways"}, "invalid_airline"),
        ({"class": "PremiumEconomy"}, "invalid_class"),
        ({"stops": "three"}, "invalid_stops"),
        ({"departure_time": "Teatime"}, "invalid_departure_time"),
        ({"source_city": "Nairobi"}, "invalid_source_city"),
        ({"duration": 31.5}, "implausible_nonstop_duration"),
        ({"price": None}, "null_price"),
        ({"flight": None}, "null_flight"),
    ],
)
def test_invalid_rows_are_rejected_with_the_right_reason(spark, overrides, expected_reason):
    assert expected_reason in reasons_for(spark, **overrides)


def test_all_failure_reasons_are_accumulated_not_short_circuited(spark):
    # A row that breaks three rules must report three reasons - otherwise fixing
    # one problem just reveals the next and the data team iterates blind.
    got = reasons_for(spark, price=-5, days_left=-2, airline="KenyaAirways")
    assert "price_out_of_range" in got
    assert "days_left_out_of_range" in got
    assert "invalid_airline" in got
    assert len(got) >= 3


# ---------------------------------------------------------------- dedupe
def test_dedupe_keeps_the_latest_ingested_row(spark):
    rows = [
        bronze_row(_source_row=1, _ingested_at=datetime(2026, 9, 9, 10, 0, 0)),
        bronze_row(_source_row=2, _ingested_at=datetime(2026, 9, 9, 18, 0, 0)),
    ]
    kept, dropped = st.deduplicate(build(spark, rows))
    assert kept.count() == 1
    assert dropped.count() == 1
    assert kept.collect()[0]["_source_row"] == 2


def test_dedupe_tiebreak_is_deterministic(spark):
    same_time = datetime(2026, 9, 9, 10, 0, 0)
    rows = [
        bronze_row(_source_row=7, _ingested_at=same_time),
        bronze_row(_source_row=3, _ingested_at=same_time),
    ]
    kept, _ = st.deduplicate(build(spark, rows))
    assert kept.collect()[0]["_source_row"] == 3  # lowest _source_row wins


def test_reconciliation_holds_valid_plus_rejected_equals_input(spark):
    rows = [bronze_row(_source_row=1), bronze_row(_source_row=2, price=-1)]
    validated = build(spark, rows)
    kept, dropped = st.deduplicate(validated)
    valid = kept.where("is_valid").count()
    rejected = kept.where("not is_valid").count() + dropped.count()
    assert valid + rejected == len(rows)
