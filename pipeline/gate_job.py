"""Batch job: find gate crossings in the clean NOAA history (data/clean), write them (data/crossings,
Parquet by date) and crossings per hour, gate, direction, and vessel group (the forecast target table)."""

import pandas as pd
from pyspark.sql import functions as F

from pipeline.config import load_config
from pipeline.gates import CROSSING_SCHEMA, find_crossings, hourly_counts
from pipeline.spark import get_spark

INPUT_COLUMNS = ["ts", "mmsi", "name", "vessel_type", "lat", "lon"]


def main() -> None:
    cfg = load_config()
    g = cfg["gates"]
    spark = get_spark(cfg, "vessel-watch-gates")
    reports = spark.read.parquet(str(cfg["paths"]["clean"]))
    days = [r.date for r in reports.select("date").distinct().collect()]
    print(f"read {reports.count():,} reports from {len(days)} days; {len(g['lines'])} gates")

    crossings = (
        reports.select(*INPUT_COLUMNS)
        .groupBy("mmsi")
        .applyInPandas(lambda vessel: find_crossings(vessel, g), schema=CROSSING_SCHEMA)
        .cache()
    )
    out = cfg["paths"]["crossings"]
    crossings.write.mode("overwrite").partitionBy("date").parquet(str(out))
    print(f"wrote {crossings.count():,} crossings to {out}")

    # Epoch seconds, not timestamps, cross to pandas: no time zone conversion on the way.
    events = crossings.select("gate", "direction", "vessel_type", F.unix_timestamp("ts").alias("epoch")).toPandas()
    events["ts"] = pd.to_datetime(events["epoch"], unit="s")
    hourly = hourly_counts(events, days, [line["name"] for line in g["lines"]], cfg["vessel_groups"])
    hourly.to_parquet(cfg["paths"]["crossings_hourly"], index=False)

    per_day = hourly.groupby(["gate", "group", "direction"])["crossings"].sum().unstack() / len(days)
    print("crossings per day:\n" + per_day.round(1).to_string())
    t = g["target"]
    target = hourly[(hourly.gate == t["gate"]) & (hourly.direction == t["direction"]) & hourly.group.isin(t["groups"])]
    per_hour = target.groupby("hour")["crossings"].sum()
    print(f"target ({t['gate']}, {t['direction']}, {'+'.join(t['groups'])}): {per_hour.mean():.2f} per hour, "
          f"{(per_hour == 0).mean():.0%} of hours zero")
    print(f"wrote {len(hourly):,} hourly counts to {cfg['paths']['crossings_hourly']}")


if __name__ == "__main__":
    main()
