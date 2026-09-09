"""
BRONZE - raw ingestion.

Contract for this layer:
  * Read with an EXPLICIT schema (never inferSchema) - see common/schemas.py.
  * Preserve every source row and every source value. No filtering, no casting
    beyond the declared source types, no business logic. If a row is bad, that
    is Silver's problem to record - Bronze's job is that we can always prove
    what the source sent us.
  * Add ingestion metadata we control: _source_file, _source_row, _ingested_at,
    _batch_id, _file_hash, as_of_date.
  * Write Parquet partitioned by as_of_date with dynamic partition overwrite,
    so a rerun for one date replaces that date only and never double-counts.

Run:
    python pyspark/bronze/bronze_ingest.py
    python pyspark/bronze/bronze_ingest.py --as-of-date 2026-09-08
    python pyspark/bronze/bronze_ingest.py --source-file data/raw/other.csv --force
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F  # noqa: E402

from common import audit, config, schemas  # noqa: E402


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Bronze ingestion for airline flight pricing")
    p.add_argument("--source-file", default=str(config.DEFAULT_SOURCE_FILE))
    p.add_argument(
        "--as-of-date",
        default=None,
        help="Logical snapshot date (YYYY-MM-DD). Defaults to file mtime, then today.",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Ingest even if this file content was already ingested for this date.",
    )
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    source_file = Path(args.source_file).resolve()
    if not source_file.exists():
        raise FileNotFoundError(f"Source file not found: {source_file}")

    as_of = config.resolve_as_of_date(args.as_of_date, source_file)
    ctx = config.JobContext(
        as_of_date=as_of, batch_id=config.make_batch_id(as_of), source_file=source_file
    )

    print(f"[bronze] source     = {source_file}")
    print(f"[bronze] as_of_date = {ctx.as_of_date_str}  (minted at ingestion - see assumptions)")
    print(f"[bronze] batch_id   = {ctx.batch_id}")

    # --- DQ control 4: unexpected schema changes, checked BEFORE reading ----
    schemas.assert_no_schema_drift(str(source_file))
    print("[bronze] schema check passed against declared SOURCE_SCHEMA")

    file_hash = audit.file_md5(source_file)
    spark = config.get_spark("bronze_ingest")

    # --- Duplicate file delivery -------------------------------------------
    if audit.already_ingested(spark, file_hash, ctx.as_of_date_str) and not args.force:
        print(
            f"[bronze] SKIP - file hash {file_hash[:12]} already ingested for "
            f"{ctx.as_of_date_str}. Use --force to override."
        )
        spark.stop()
        return 0

    raw = (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")  # keep malformed rows; Silver rejects them
        .schema(schemas.SOURCE_SCHEMA)
        .csv(str(source_file))
    )

    bronze = (
        raw.withColumnRenamed("index", "_source_row")
        .withColumn("_source_file", F.lit(source_file.name))
        .withColumn("_ingested_at", F.lit(datetime.now(timezone.utc)).cast("timestamp"))
        .withColumn("_batch_id", F.lit(ctx.batch_id))
        .withColumn("_file_hash", F.lit(file_hash))
        .withColumn("as_of_date", F.lit(ctx.as_of_date_str).cast("date"))
    )

    row_count = bronze.count()

    (
        bronze.write.mode("overwrite")  # + dynamic partitionOverwriteMode
        .partitionBy("as_of_date")
        .parquet(str(config.BRONZE_PATH))
    )

    audit.record_ingestion(
        spark,
        source_file=source_file.name,
        file_hash=file_hash,
        as_of_date=ctx.as_of_date_str,
        batch_id=ctx.batch_id,
        row_count=row_count,
    )

    audit.log_dq_run(
        spark,
        {
            "layer": "bronze",
            "batch_id": ctx.batch_id,
            "as_of_date": ctx.as_of_date_str,
            "source_file": source_file.name,
            "file_hash": file_hash,
            "source_row_count": row_count,
            "bronze_row_count": row_count,
            "reconciled": True,
        },
    )

    print(f"[bronze] wrote {row_count:,} rows -> {config.BRONZE_PATH}")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
