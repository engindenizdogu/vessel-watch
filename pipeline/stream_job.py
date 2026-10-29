"""Structured Streaming job: read the raw topic, apply the stateless cleaning steps, and append
position reports to serving Parquet. Duplicate and spike removal need per-vessel state (week 3).
"""

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType

from pipeline.clean import stateless_steps
from pipeline.config import load_config
from pipeline.spark import get_spark

# Shared message schema emitted by both producers (see ingest/live_client.py).
RAW_SCHEMA = StructType([
    StructField("ts", StringType()),
    StructField("mmsi", LongType()),
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

    query = (
        positions.writeStream.format("parquet")
        .option("path", str(cfg["paths"]["positions"]))
        .option("checkpointLocation", str(cfg["paths"]["checkpoints"] / f"positions_{args.run}"))
        .partitionBy("date")
        .trigger(processingTime=f"{cfg['stream']['trigger_seconds']} seconds")
        .start()
    )
    print(f"Streaming {cfg['kafka']['topic_raw']} -> {cfg['paths']['positions']}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
