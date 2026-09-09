"""
Unit tests for the schema-drift contract (no Spark needed - fast).

    pytest tests/test_schema_drift.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "pyspark"))

from common import schemas  # noqa: E402


def test_declared_schema_matches_the_supplied_file():
    header = schemas.read_header(str(REPO_ROOT / "data" / "raw" / "airlines_flights_data.csv"))
    assert header == schemas.SOURCE_COLUMNS
    assert schemas.detect_schema_drift(header) == {}


def test_missing_column_is_drift():
    header = [c for c in schemas.SOURCE_COLUMNS if c != "price"]
    drift = schemas.detect_schema_drift(header)
    assert drift["missing_columns"] == ["price"]


def test_new_column_is_drift():
    drift = schemas.detect_schema_drift(schemas.SOURCE_COLUMNS + ["fare_basis_code"])
    assert drift["unexpected_columns"] == ["fare_basis_code"]


def test_reordering_is_reported_separately_from_structural_drift():
    reordered = list(reversed(schemas.SOURCE_COLUMNS))
    drift = schemas.detect_schema_drift(reordered)
    assert "reordered_columns" in drift
    assert "missing_columns" not in drift
    assert "unexpected_columns" not in drift


def test_drift_check_fails_loudly_on_a_missing_column(tmp_path):
    bad = tmp_path / "bad.csv"
    bad.write_text("airline,flight,price\nVistara,UK-995,5955\n")
    with pytest.raises(schemas.SchemaDriftError) as exc:
        schemas.assert_no_schema_drift(str(bad))
    assert "missing_columns" in str(exc.value)


def test_reorder_is_tolerated_when_allowed(tmp_path):
    reordered = list(reversed(schemas.SOURCE_COLUMNS))
    good = tmp_path / "reordered.csv"
    good.write_text(",".join(reordered) + "\n")
    drift = schemas.assert_no_schema_drift(str(good), allow_reorder=True)
    assert "reordered_columns" in drift  # warned, not raised


def test_reorder_can_be_made_fatal(tmp_path):
    reordered = list(reversed(schemas.SOURCE_COLUMNS))
    f = tmp_path / "reordered.csv"
    f.write_text(",".join(reordered) + "\n")
    with pytest.raises(schemas.SchemaDriftError):
        schemas.assert_no_schema_drift(str(f), allow_reorder=False)
