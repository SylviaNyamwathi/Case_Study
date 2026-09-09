"""
Build a small demo file with deliberately broken rows.

Why this exists: the supplied dataset is clean - 300,153 rows, zero nulls, zero
exact duplicates, every categorical inside its accepted set. A pipeline whose
reject path never fires is a pipeline nobody has actually tested. This script
takes a sample of real rows and injects one row per failure mode, so
`silver_rejected/` can be shown populated, with a correct `rejection_reason` on
every row.

    python scripts/make_dq_demo_file.py
    python pyspark/run_pipeline.py \
        --source-file data/raw/samples/flights_dq_demo.csv \
        --as-of-date 2026-09-10

Expected: 9 rejected rows covering 9 distinct reasons (see
documentation/data_quality.md).
"""

from __future__ import annotations

import csv
import itertools
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE = REPO_ROOT / "data" / "raw" / "airlines_flights_data.csv"
OUT_DIR = REPO_ROOT / "data" / "raw" / "samples"
OUT = OUT_DIR / "flights_dq_demo.csv"

SAMPLE_ROWS = 2000

# (rejection reason it should trigger, {column: bad value})
INJECTIONS: list[tuple[str, dict[str, str]]] = [
    ("price_out_of_range",           {"price": "0"}),
    ("price_out_of_range",           {"price": "-4500"}),
    ("duration_out_of_range",        {"duration": "0"}),
    ("days_left_out_of_range",       {"days_left": "-3"}),
    ("same_source_and_destination",  {"destination_city": "Delhi", "source_city": "Delhi"}),
    ("invalid_airline",              {"airline": "KenyaAirways"}),
    ("invalid_class",                {"class": "PremiumEconomy"}),
    ("invalid_stops",                {"stops": "three"}),
    ("implausible_nonstop_duration", {"stops": "zero", "duration": "31.5"}),
]


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    with SOURCE.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        header = reader.fieldnames
        assert header is not None
        rows = [dict(r) for r in itertools.islice(reader, SAMPLE_ROWS)]

    next_index = max(int(r["index"]) for r in rows) + 1
    for offset, (_reason, overrides) in enumerate(INJECTIONS):
        bad = dict(rows[offset])          # start from a real, valid row
        bad["index"] = str(next_index + offset)
        bad["flight"] = f"DQ-{offset:03d}"  # so the demo rows are easy to find
        bad.update(overrides)
        rows.append(bad)

    with OUT.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)

    print(f"wrote {len(rows):,} rows ({len(INJECTIONS)} deliberately invalid) -> {OUT}")
    print("expected rejection reasons:")
    for reason, overrides in INJECTIONS:
        print(f"  {reason:<32} {overrides}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
