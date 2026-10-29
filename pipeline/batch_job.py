"""Batch job: clean the NOAA daily CSVs in data/raw into date-partitioned Parquet in data/clean."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType

from pipeline.clean import batch_steps
from pipeline.config import load_config
from pipeline.spark import get_spark

# Columns as published in AIS_2024_MM_DD.csv (verified against AIS_2024_07_01, see docs/SOURCES.md).
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


def main() -> None:
    cfg = load_config()
    spark = get_spark(cfg, "vessel-watch-batch")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")  # rewrite only the dates we read

    raw_glob = str(cfg["paths"]["raw"] / "AIS_*.csv")
    df = to_shared_schema(spark.read.csv(raw_glob, header=True, schema=NOAA_SCHEMA))
    print(f"read {raw_glob}: {df.count():,} rows")

    for i, (name, step) in enumerate(batch_steps(cfg)):
        df = step(df)
        if i == 0:
            df = df.cache()  # the harbor subset is small; later counts reuse it instead of rereading the CSVs
        print(f"{name}: {df.count():,} rows")

    out = cfg["paths"]["clean"]
    df.withColumn("date", F.to_date("ts")).write.mode("overwrite").partitionBy("date").parquet(str(out))
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
