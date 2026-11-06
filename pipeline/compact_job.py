"""Compaction: move live reports from data/live/incoming (small files from the streaming history sink) into
data/live/history (one file per day), running the cleaning steps the stream skips (duplicates, position
spikes), and delete serving positions older than `stream.serving_days`.

`make stream` runs `compact` every `stream.compact_minutes` on the live feed, today included, so history
lags by at most one run. Only the files read are deleted; reports written meanwhile wait for the next run.
`make compact` runs it once, for when the stream is not running (two runs at once would race).
"""

import shutil
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipeline.clean import stateful_steps
from pipeline.config import load_config
from pipeline.spark import get_spark


def partition_days(folder: Path) -> dict[date, Path]:
    """`date=YYYY-MM-DD` partition folders under `folder`, by date."""
    return {date.fromisoformat(p.name.removeprefix("date=")): p for p in sorted(folder.glob("date=*"))}


def compact_day(spark: SparkSession, cfg: dict[str, Any], files: list[Path], day: date, history: Path) -> int:
    """Merge `files` (one day of incoming live reports) with that day's history, clean, and rewrite it as one file.

    Returns the number of rows written.
    """
    df = spark.read.option("mergeSchema", "true").parquet(*map(str, files))  # older files may lack newer columns
    print(f"{day}: {df.count():,} new reports from {len(files)} files")
    existing = history / f"date={day}"
    if existing.exists():
        df = df.unionByName(spark.read.parquet(str(existing)), allowMissingColumns=True)
        print(f"{day}: {df.count():,} with the day's existing history")
    for name, step in stateful_steps(cfg):
        df = step(df)
        print(f"{day}: {name}: {df.count():,} rows")
    df = df.localCheckpoint()  # materialize before overwriting the partition it may have been read from
    df.withColumn("date", F.lit(day)).coalesce(1).write.mode("overwrite").partitionBy("date").parquet(str(history))
    return df.count()


def delete_files(files: list[Path]) -> None:
    """Delete Parquet files and the hidden .crc checksums Hadoop writes next to them; drop emptied folders."""
    for f in files:
        f.unlink()
        f.with_name(f".{f.name}.crc").unlink(missing_ok=True)
    for folder in {f.parent for f in files}:
        if not any(folder.iterdir()):
            folder.rmdir()


def prune_serving(folder: Path, today: date, keep_days: int) -> list[date]:
    """Delete serving partitions older than `keep_days` days (today included); return the dates deleted."""
    cutoff = today - timedelta(days=keep_days - 1)
    old = [d for d in partition_days(folder) if d < cutoff]
    for d in old:
        shutil.rmtree(folder / f"date={d}")
    return old


def compact(spark: SparkSession, cfg: dict[str, Any], today: date) -> None:
    """Compact every day waiting in incoming into history, then trim serving days before the kept window."""
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    paths = cfg["paths"]
    for day, folder in partition_days(paths["live_incoming"]).items():
        files = sorted(folder.glob("*.parquet"))
        if files:
            rows = compact_day(spark, cfg, files, day, paths["live_history"])
            delete_files(files)
            print(f"{day}: live history now {rows:,} reports")
    deleted = prune_serving(paths["positions"], today, cfg["stream"]["serving_days"])
    if deleted:
        print(f"deleted serving days: {', '.join(map(str, deleted))}")


def main() -> None:
    cfg = load_config()
    compact(get_spark(cfg, "vessel-watch-compact"), cfg, datetime.now(timezone.utc).date())


if __name__ == "__main__":
    main()
