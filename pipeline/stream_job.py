"""Structured Streaming job: read the raw topic, apply the stateless cleaning steps, and append
position reports to serving Parquet. On the live feed, a second query with its own checkpoint (the training
sink) also stores every report as live history; `make compact` later merges it per day and runs the
duplicate and spike steps, which need each vessel's other reports.
"""

import argparse
import threading
import time
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType

from pipeline.clean import stateless_steps
from pipeline.compact_job import compact
from pipeline.config import load_config
from pipeline.spark import get_spark

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
    spark = get_spark(cfg, "vessel-watch-stream", packages=[cfg["kafka"]["spark_package"]])

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg["kafka"]["bootstrap_servers"])
        .option("subscribe", cfg["kafka"]["topic_raw"])
        .option("startingOffsets", "earliest")
        .load()
    )
    positions = parse_raw(kafka_df)
    for _, step in stateless_steps(cfg):
        positions = step(positions)

    s = cfg["stream"]
    (
        positions.coalesce(1)  # one file per micro-batch (per date) instead of one per Kafka partition
        .writeStream.format("parquet")
        .option("path", str(cfg["paths"]["positions"]))
        .option("checkpointLocation", str(cfg["paths"]["checkpoints"] / f"positions_{args.run}"))
        .partitionBy("date")
        .trigger(processingTime=f"{s['trigger_seconds']} seconds")
        .start()
    )
    print(f"Streaming {cfg['kafka']['topic_raw']} -> {cfg['paths']['positions']}")

    if args.run == "live":  # replayed NOAA data is already in data/clean; keep it out of live history
        recent = positions.where(F.col("ts") >= F.expr(f"current_timestamp() - INTERVAL {s['history_max_age_days']} DAYS"))
        (
            recent.coalesce(1)
            .writeStream.format("parquet")
            .option("path", str(cfg["paths"]["live_incoming"]))
            .option("checkpointLocation", str(cfg["paths"]["checkpoints"] / "live_history"))
            .partitionBy("date")
            .trigger(processingTime=f"{s['history_trigger_seconds']} seconds")
            .start()
        )
        print(f"Live history: {cfg['kafka']['topic_raw']} -> {cfg['paths']['live_incoming']} (from the earliest message Kafka keeps)")
        threading.Thread(target=compact_every, args=(spark, cfg, s["compact_minutes"]), daemon=True).start()
        print(f"Compacting live history into {cfg['paths']['live_history']} every {s['compact_minutes']} min")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
