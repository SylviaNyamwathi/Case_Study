"""
Convenience orchestrator: Bronze then Silver, in one process.

In production this is an Airflow/Step Functions DAG with each layer as its own
task (separate retries, separate SLAs). It is a single script here only so the
whole pipeline can be demonstrated with one command.

    python pyspark/run_pipeline.py
    python pyspark/run_pipeline.py --as-of-date 2026-09-08 --force
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from bronze import bronze_ingest  # noqa: E402
from silver import silver_transform  # noqa: E402


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Run Bronze -> Silver end to end")
    p.add_argument("--source-file", default=None)
    p.add_argument("--as-of-date", default=None)
    p.add_argument("--force", action="store_true")
    args = p.parse_args(argv)

    bronze_args = []
    if args.source_file:
        bronze_args += ["--source-file", args.source_file]
    if args.as_of_date:
        bronze_args += ["--as-of-date", args.as_of_date]
    if args.force:
        bronze_args += ["--force"]

    print("\n########## BRONZE ##########")
    rc = bronze_ingest.main(bronze_args)
    if rc != 0:
        return rc

    silver_args = ["--as-of-date", args.as_of_date] if args.as_of_date else []
    print("\n########## SILVER ##########")
    return silver_transform.main(silver_args)


if __name__ == "__main__":
    raise SystemExit(main())
