"""
Audit helpers: file manifest (duplicate-delivery guard) and DQ run log
(source-to-target reconciliation).

Both are written as Parquet tables in the lake so they are queryable by dbt /
Redshift Spectrum exactly like any other dataset. In production these would be
small Delta/Iceberg tables or a Postgres control schema.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import Row, SparkSession

from . import config


def file_md5(path: str | Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    """Streamed md5 of the source file - identifies duplicate file delivery
    even when the filename changes."""
    digest = hashlib.md5()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_table(spark: SparkSession, path: Path):
    if not path.exists():
        return None
    try:
        return spark.read.parquet(str(path))
    except Exception:  # empty or not yet written
        return None


def already_ingested(spark: SparkSession, file_hash: str, as_of_date: str) -> bool:
    """True if this exact file content was already ingested for this as_of_date.

    This is the duplicate-file-delivery control: the same file re-sent (possibly
    renamed) is a no-op, while genuinely new content for the same date is
    allowed through.
    """
    manifest = _read_table(spark, config.MANIFEST_PATH)
    if manifest is None:
        return False
    return (
        manifest.where(
            (manifest._file_hash == file_hash) & (manifest.as_of_date == as_of_date)
        ).limit(1).count()
        > 0
    )


def record_ingestion(
    spark: SparkSession,
    *,
    source_file: str,
    file_hash: str,
    as_of_date: str,
    batch_id: str,
    row_count: int,
) -> None:
    """Append one row to the ingestion manifest."""
    row = Row(
        _source_file=source_file,
        _file_hash=file_hash,
        as_of_date=as_of_date,
        _batch_id=batch_id,
        source_row_count=int(row_count),
        _ingested_at=datetime.now(timezone.utc),
    )
    config.MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    spark.createDataFrame([row]).write.mode("append").parquet(str(config.MANIFEST_PATH))


def log_dq_run(spark: SparkSession, metrics: dict) -> None:
    """Append one row of per-run DQ metrics, and echo them to stdout.

    The echo matters in a review: the panel can see reconciliation pass/fail
    without opening a Parquet file.
    """
    print("\n=== DQ RUN LOG ===")
    print(json.dumps(metrics, indent=2, default=str))
    print("==================\n")

    payload = {k: (v if not isinstance(v, (dict, list)) else json.dumps(v)) for k, v in metrics.items()}
    payload.setdefault("_logged_at", datetime.now(timezone.utc))
    config.DQ_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    spark.createDataFrame([Row(**payload)]).write.mode("append").parquet(
        str(config.DQ_LOG_PATH)
    )
