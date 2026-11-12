"""Structured Streaming job: read the raw topic, apply the stateless cleaning steps, and write to the run's
serving folder (`serving_dir`), each query with its own checkpoint:
- positions: every report, for the map;
- gap_events: signal gaps from per-vessel state (`models.gaps.track_gaps`, event-time timeout);
- minute_counts: reports per minute, which the feed-outage guard reads (`models.gaps.confirmed_gaps`);
- offroute: off-route reports (`models.offroute.flag_offroute`, joined with the lanes `make offroute` learned;
  skipped until that file exists, and read once at start).
On the live feed, another query (the training sink) also stores every report as live history; `make compact`
later merges it per day and runs the duplicate and spike steps, which need each vessel's other reports.

Live and replay share the topic, so each run keeps only its own era by event time: live keeps reports from
the last `stream.live_max_age_days`, replay keeps older ones. A replay run starts fresh each time and reads
only messages produced after it starts.
"""

import argparse
import shutil
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.streaming.state import GroupStateTimeout
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType

from models.gaps import GAP_SCHEMA, REPORT_COLUMNS, STATE_SCHEMA, track_gaps
from models.offroute import flag_offroute
from pipeline.clean import stateless_steps
from pipeline.compact_job import compact
from pipeline.config import load_config, serving_dir
from pipeline.spark import get_spark

OUTPUTS = ("positions", "gap_events", "minute_counts", "offroute")  # folders in the run's serving folder, one query each

# Shared message schema emitted by both producers (see ingest/live_client.py).
RAW_SCHEMA = StructType([
    StructField("ts", StringType()),
    StructField("mmsi", LongType()),
    StructField("name", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("sog", DoubleType()),
    StructField("cog", DoubleType()),
    StructField("heading", IntegerType()),
    StructField("vessel_type", IntegerType()),
    StructField("status", IntegerType()),
])


def parse_raw(kafka_df: DataFrame) -> DataFrame:
    """Turn Kafka rows into typed position reports with a UTC event timestamp and a date column."""
    return (
        kafka_df
        .select(F.from_json(F.col("value").cast("string"), RAW_SCHEMA).alias("r"))
        .select("r.*")
        .withColumn("ts", F.to_timestamp("ts"))
        .withColumn("date", F.to_date("ts"))
    )


def keep_run(positions: DataFrame, run: str, live_max_age_days: int) -> DataFrame:
    """Keep the reports that belong to `run`: live keeps the last `live_max_age_days` days, replay older ones."""
    recent = F.col("ts") >= F.expr(f"current_timestamp() - INTERVAL {live_max_age_days} DAYS")
    return positions.where(recent if run == "live" else ~recent)


def gap_events(positions: DataFrame, cfg: dict[str, Any]) -> DataFrame:
    """Signal-gap events: the gap rule per vessel with state and an event-time timeout. Open gaps fire once the
    watermark passes their alert time; the watermark trails the newest report by `stream.watermark_seconds`,
    and later reports are dropped as late."""
    return (
        positions.withWatermark("ts", f"{cfg['stream']['watermark_seconds']} seconds")
        .select(*REPORT_COLUMNS)
        .groupBy("mmsi")
        .applyInPandasWithState(
            lambda key, batches, state: track_gaps(key, batches, state, cfg),
            outputStructType=GAP_SCHEMA,
            stateStructType=STATE_SCHEMA,
            outputMode="append",
            timeoutConf=GroupStateTimeout.EventTimeTimeout,
        )
        # Computing a column in Spark adds a projection: the Python state operator emits columnar rows, which the
        # file writer's sort by date cannot take (ClassCastException to UnsafeRow in Spark 3.5).
        .withColumn("date", F.to_date("last_ts"))
    )


def minute_counts(positions: DataFrame, cfg: dict[str, Any]) -> DataFrame:
    """Reports per minute, written once the watermark passes the minute (minutes without reports are absent)."""
    return (
        positions.withWatermark("ts", f"{cfg['stream']['watermark_seconds']} seconds")
        .groupBy(F.window("ts", "1 minute"))
        .count()
        .select(F.col("window.start").alias("minute"), F.col("count").alias("reports"),
                F.to_date("window.start").alias("date"))
    )


def write_parquet(df: DataFrame, path: Path, checkpoint: Path, trigger_seconds: int) -> StreamingQuery:
    """Append a stream to date-partitioned Parquet, one file per micro-batch and date; the query is named after
    the output folder."""
    return (
        df.coalesce(1)  # instead of one file per Kafka or shuffle partition
        .writeStream.format("parquet")
        .queryName(path.name)
        .option("path", str(path))
        .option("checkpointLocation", str(checkpoint))
        .partitionBy("date")
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )


def log_late_reports(queries: list[StreamingQuery], every_seconds: int) -> None:
    """Print how many reports each micro-batch dropped as too late for the watermark (otherwise silent)."""
    done: dict[str, int] = {}
    while True:
        time.sleep(every_seconds)
        for q in queries:
            for p in q.recentProgress:
                if p["batchId"] <= done.get(q.name, -1):
                    continue
                done[q.name] = p["batchId"]
                late = sum(op.get("numRowsDroppedByWatermark", 0) for op in p["stateOperators"])
                if late:
                    print(f"{q.name}: batch {p['batchId']} dropped {late:,} late reports (watermark {p['eventTime'].get('watermark')})")


def compact_every(spark: SparkSession, cfg: dict[str, Any], minutes: int) -> None:
    """Compact live history now and then every `minutes`; a failed run is logged and retried, not fatal."""
    while True:
        try:
            compact(spark, cfg, datetime.now(timezone.utc).date())
        except Exception as e:  # keep streaming; the files stay in incoming for the next run
            print(f"live history compaction failed, retrying in {minutes} min: {e}")
        time.sleep(minutes * 60)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", default="live", choices=["live", "replay"],
                        help="which producer is feeding the topic; each run gets its own checkpoint")
    args = parser.parse_args()

    cfg = load_config()
    paths, s = cfg["paths"], cfg["stream"]
    live = args.run == "live"
    out = serving_dir(cfg, args.run)
    checkpoints = {name: paths["checkpoints"] / f"{name}_{args.run}" for name in OUTPUTS}
    if not live:
        # A replay is a session, not a feed to resume. Outputs go with their checkpoints: a new checkpoint
        # restarts batch ids at 0, and Spark skips batches that an output's old _spark_metadata already lists.
        for folder in (out, *checkpoints.values()):
            shutil.rmtree(folder, ignore_errors=True)
        print(f"Replay run: cleared {out} and its checkpoints")

    spark = get_spark(
        cfg, "vessel-watch-stream", packages=[cfg["kafka"]["spark_package"]], driver_memory=cfg["stream"]["driver_memory"]
    )
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg["kafka"]["bootstrap_servers"])
        .option("subscribe", cfg["kafka"]["topic_raw"])
        # Live resumes from the earliest message Kafka keeps; replay skips earlier replays still in the topic.
        .option("startingOffsets", "earliest" if live else "latest")
        .load()
    )
    positions = parse_raw(kafka_df)
    for _, step in stateless_steps(cfg):
        positions = step(positions)
    positions = keep_run(positions, args.run, s["live_max_age_days"])

    streams = {"positions": positions, "gap_events": gap_events(positions, cfg),
               "minute_counts": minute_counts(positions, cfg)}
    if paths["ship_lanes"].exists():
        streams["offroute"] = flag_offroute(positions, spark.read.parquet(str(paths["ship_lanes"])), cfg)
    else:
        print(f"No {paths['ship_lanes']} yet (make offroute): off-route alerts are off")
    queries = [write_parquet(df, out / name, checkpoints[name], s["trigger_seconds"]) for name, df in streams.items()]
    print(f"Streaming {cfg['kafka']['topic_raw']} -> {', '.join(str(out / name) for name in streams)}")
    threading.Thread(target=log_late_reports, args=(queries, s["trigger_seconds"]), daemon=True).start()

    if live:
        write_parquet(positions, paths["live_incoming"], paths["checkpoints"] / "live_history", s["history_trigger_seconds"])
        print(f"Live history: {cfg['kafka']['topic_raw']} -> {paths['live_incoming']} (from the earliest message Kafka keeps)")
        threading.Thread(target=compact_every, args=(spark, cfg, s["compact_minutes"]), daemon=True).start()
        print(f"Compacting live history into {paths['live_history']} every {s['compact_minutes']} min")
    else:
        # A query's first progress means its start offsets are fixed.
        while any(q.isActive and q.lastProgress is None for q in queries):
            time.sleep(1)
        print("Ready: start the replay producer now (make replay START=YYYY-MM-DD)")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
