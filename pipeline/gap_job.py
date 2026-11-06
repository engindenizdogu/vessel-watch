"""Batch job: find signal gaps in the clean NOAA history (data/clean) and write them to data/gaps."""

from pyspark.sql import functions as F

from models.gaps import GAP_SCHEMA, coverage_ends, find_gaps
from pipeline.config import load_config
from pipeline.spark import get_spark

INPUT_COLUMNS = ["ts", "mmsi", "name", "lat", "lon", "sog", "vessel_type", "status"]


def main() -> None:
    cfg = load_config()
    spark = get_spark(cfg, "vessel-watch-gaps")
    reports = spark.read.parquet(str(cfg["paths"]["clean"]))
    days = [r.date for r in reports.select("date").distinct().collect()]
    ends = coverage_ends(days)
    print(f"read {reports.count():,} reports from {len(days)} days in {len(set(ends.values()))} run(s) of consecutive days")

    gaps = (
        reports.select(*INPUT_COLUMNS)
        .groupBy("mmsi")
        .applyInPandas(lambda vessel: find_gaps(vessel, cfg, ends), schema=GAP_SCHEMA)
        .cache()
    )
    summary = gaps.groupBy("state", F.col("resumed_ts").isNotNull().alias("resumed")).count().orderBy("state", "resumed")
    for row in summary.collect():
        print(f"{row.state} gaps, {'resumed' if row.resumed else 'open (not seen again before the data ends)'}: {row['count']:,}")

    out = cfg["paths"]["gaps"]
    gaps.write.mode("overwrite").partitionBy("date").parquet(str(out))
    print(f"wrote {gaps.count():,} gaps from {gaps.select('mmsi').distinct().count():,} vessels to {out}")


if __name__ == "__main__":
    main()
