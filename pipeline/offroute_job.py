"""Batch job: learn ship lanes from the training days of the clean history (before `split.test_start`), write
them (data/models/ship_lanes.parquet, read by the streaming job), and flag off-route reports on the test days
(data/offroute, Parquet by date), printing the alert rate on that unseen traffic."""

import pandas as pd
from pyspark.sql import functions as F

from models.offroute import checked_reports, episodes, flag_offroute, learn_lanes
from pipeline.config import load_config
from pipeline.spark import get_spark


def main() -> None:
    cfg = load_config()
    o, test_start = cfg["offroute"], cfg["split"]["test_start"]
    spark = get_spark(cfg, "vessel-watch-offroute")
    reports = spark.read.parquet(str(cfg["paths"]["clean"]))
    train, test = reports.where(F.col("date") < F.lit(test_start)), reports.where(F.col("date") >= F.lit(test_start))

    lanes = learn_lanes(train, cfg).cache()
    path = cfg["paths"]["ship_lanes"]
    lanes.toPandas().to_parquet(path, index=False)
    print(f"learned {lanes.count():,} lane cells ({o['cell_deg']} deg, with neighbours) from "
          f"{checked_reports(train, cfg).select('mmsi').distinct().count():,} ships under way before {test_start}; wrote {path}")

    flagged = flag_offroute(test, lanes, cfg).cache()
    out = cfg["paths"]["offroute"]
    flagged.write.mode("overwrite").partitionBy("date").parquet(str(out))
    checked = checked_reports(test, cfg).count()
    n = flagged.count()
    print(f"test days ({test_start} on): {n:,} of {checked:,} checked reports off-route ({n / checked:.2%}); wrote {out}")

    # Epoch seconds, not timestamps, cross to pandas: no time zone conversion on the way.
    rows = flagged.select("mmsi", "name", "vessel_type", "lat", "lon", "lane_ships", F.unix_timestamp("ts").alias("epoch")).toPandas()
    rows["ts"] = pd.to_datetime(rows.pop("epoch"), unit="s")
    alerts = episodes(rows, o["episode_gap_minutes"])
    days = test.select("date").distinct().count()
    print(f"{len(alerts)} alerts ({len(alerts) / days:.1f} per day over {days} days) from {alerts['mmsi'].nunique()} ships:")
    print(alerts.round(3).to_string(index=False))


if __name__ == "__main__":
    main()
