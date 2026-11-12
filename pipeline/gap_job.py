"""Batch job: find signal gaps in the clean NOAA history (data/clean), learn shutdown spots from them, and
write both (data/gaps, and the spots file under data/models)."""

from pyspark.sql import functions as F

from models.gaps import (
    GAP_SCHEMA, LABELED_GAP_SCHEMA, REPORT_COLUMNS, coverage_ends, feed_down_minutes, find_gaps, label_known_spots,
    shutdown_spots,
)
from pipeline.config import load_config
from pipeline.spark import get_spark, reports_per_minute


def main() -> None:
    cfg = load_config()
    g = cfg["signal_gap"]
    spark = get_spark(cfg, "vessel-watch-gaps")
    reports = spark.read.parquet(str(cfg["paths"]["clean"]))
    days = [r.date for r in reports.select("date").distinct().collect()]
    ends = coverage_ends(days)
    print(f"read {reports.count():,} reports from {len(days)} days in {len(set(ends.values()))} run(s) of consecutive days")

    down = feed_down_minutes(reports_per_minute(reports), days, g["outage_fraction"])
    print(
        f"feed outage in {len(down):,} of {len(days) * 1440:,} minutes (under {g['outage_fraction']:.0%} of the "
        "day's median reports per minute); no gap is raised across them"
    )

    gaps = (
        reports.select(*REPORT_COLUMNS)
        .groupBy("mmsi")
        .applyInPandas(lambda vessel: find_gaps(vessel, cfg, ends, down), schema=GAP_SCHEMA)
        .cache()
    )
    spots = shutdown_spots(gaps.select("mmsi", "lat", "lon", "gap_s", "moved_km").toPandas(), g)
    spots_path = cfg["paths"]["shutdown_spots"]
    spots_path.parent.mkdir(parents=True, exist_ok=True)
    spots.to_parquet(spots_path, index=False)
    print(f"wrote {len(spots)} shutdown spots to {spots_path}")

    labeled = gaps.mapInPandas(
        lambda batches: (label_known_spots(b, spots, g) for b in batches), schema=LABELED_GAP_SCHEMA
    ).cache()
    summary = (
        labeled.groupBy("state", F.col("resumed_ts").isNotNull().alias("resumed"), "known_spot")
        .count().orderBy("state", "resumed", "known_spot")
    )
    for row in summary.collect():
        where = "at a shutdown spot" if row.known_spot else "elsewhere"
        resumed = "resumed" if row.resumed else "open (not seen again before the data ends)"
        print(f"{row.state} gaps, {resumed}, {where}: {row['count']:,}")

    out = cfg["paths"]["gaps"]
    labeled.write.mode("overwrite").partitionBy("date").parquet(str(out))
    print(f"wrote {labeled.count():,} gaps from {labeled.select('mmsi').distinct().count():,} vessels to {out}")


if __name__ == "__main__":
    main()
