"""Batch job: clean the NOAA daily CSVs in data/raw into date-partitioned Parquet in data/clean."""

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType

from pipeline.clean import batch_steps
from pipeline.config import load_config
from pipeline.spark import get_spark

# Columns as published in the daily CSVs (verified against AIS_2024_07_01 and AIS_2023_03_14, see docs/SOURCES.md).
NOAA_SCHEMA = StructType([
    StructField("MMSI", LongType()),
    StructField("BaseDateTime", StringType()),
    StructField("LAT", DoubleType()),
    StructField("LON", DoubleType()),
    StructField("SOG", DoubleType()),
    StructField("COG", DoubleType()),
    StructField("Heading", DoubleType()),
    StructField("VesselName", StringType()),
    StructField("IMO", StringType()),
    StructField("CallSign", StringType()),
    StructField("VesselType", IntegerType()),
    StructField("Status", IntegerType()),
    StructField("Length", IntegerType()),
    StructField("Width", IntegerType()),
    StructField("Draft", DoubleType()),
    StructField("Cargo", IntegerType()),
    StructField("TransceiverClass", StringType()),
])


def to_shared_schema(noaa: DataFrame) -> DataFrame:
    """Rename and cast NOAA columns to the shared message schema (BaseDateTime is UTC)."""
    return noaa.select(
        F.to_timestamp("BaseDateTime").alias("ts"),
        F.col("MMSI").alias("mmsi"),
        F.col("LAT").alias("lat"),
        F.col("LON").alias("lon"),
        F.col("SOG").alias("sog"),
        F.col("COG").alias("cog"),
        F.col("Heading").cast("int").alias("heading"),
        F.col("VesselType").alias("vessel_type"),
        F.col("Status").alias("status"),
    )


def clean_csvs(spark: SparkSession, cfg: dict[str, Any], paths: list[str]) -> None:
    """Clean NOAA daily CSVs (paths or globs) into data/clean, rewriting only the dates they contain."""
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    df = to_shared_schema(spark.read.csv(paths, header=True, schema=NOAA_SCHEMA))
    print(f"read {', '.join(paths)}: {df.count():,} rows")

    for i, (name, step) in enumerate(batch_steps(cfg)):
        df = step(df)
        if i == 0:
            df = harbor = df.cache()  # the harbor subset is small; later counts reuse it instead of rereading the CSVs
        print(f"{name}: {df.count():,} rows")

    out = cfg["paths"]["clean"]
    df.withColumn("date", F.to_date("ts")).write.mode("overwrite").partitionBy("date").parquet(str(out))
    harbor.unpersist()  # the history job cleans many days in one session
    print(f"wrote {out}")


def main() -> None:
    cfg = load_config()
    spark = get_spark(cfg, "vessel-watch-batch")
    clean_csvs(spark, cfg, [str(cfg["paths"]["raw"] / "AIS_*.csv")])


if __name__ == "__main__":
    main()
