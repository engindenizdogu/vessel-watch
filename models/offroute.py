"""Off-route rule, shared by the batch job and the streaming job: a cargo ship or tanker under way where (almost)
no such ship moved in the training history.

The harbor is cut into a grid of `cell_deg` cells with native Spark expressions (no Python UDFs). The lanes
table holds, for each cell, the most distinct checked ships that moved through the cell or any of its 8
neighbours in the training days; the neighbours keep a ship slightly wide of a lane from alerting. A checked
report in a cell with fewer than `min_ships` is off-route. Parameters are in the `offroute` section of
config.yaml. Flagged reports of one ship close together in time form one alert (`episodes`).
"""

from typing import Any

import pandas as pd
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

LANE_COLUMNS = ["cy", "cx", "ships"]
EPISODE_COLUMNS = ["mmsi", "name", "vessel_type", "start", "end", "reports", "lat", "lon", "min_lane_ships"]


def in_groups(vessel_type: Column, cfg: dict[str, Any]) -> Column:
    """True where the AIS type is in one of the checked vessel groups (`offroute.groups`, ranges in `vessel_groups`)."""
    ranges = [r for name in cfg["offroute"]["groups"] for r in cfg["vessel_groups"][name]]
    hit = F.lit(False)
    for lo, hi in ranges:
        hit = hit | vessel_type.between(lo, hi)
    return F.coalesce(hit, F.lit(False))  # no type: not checked


def checked_reports(reports: DataFrame, cfg: dict[str, Any]) -> DataFrame:
    """Reports the rule applies to (checked groups, under way), with their grid cell (`cy`, `cx`)."""
    o = cfg["offroute"]
    return (
        reports.where(in_groups(F.col("vessel_type"), cfg) & (F.col("sog") >= o["min_knots"]))
        .withColumn("cy", F.floor(F.col("lat") / o["cell_deg"]).cast("int"))
        .withColumn("cx", F.floor(F.col("lon") / o["cell_deg"]).cast("int"))
    )


def learn_lanes(reports: DataFrame, cfg: dict[str, Any]) -> DataFrame:
    """Lanes table (LANE_COLUMNS): per cell, the most distinct checked ships under way in it or a neighbour."""
    per_cell = checked_reports(reports, cfg).groupBy("cy", "cx").agg(F.countDistinct("mmsi").alias("ships"))
    offsets = per_cell.sparkSession.range(-1, 2).select(F.col("id").cast("int").alias("dy")).crossJoin(
        per_cell.sparkSession.range(-1, 2).select(F.col("id").cast("int").alias("dx"))
    )
    return (
        per_cell.crossJoin(F.broadcast(offsets))
        .select((F.col("cy") + F.col("dy")).alias("cy"), (F.col("cx") + F.col("dx")).alias("cx"), "ships")
        .groupBy("cy", "cx")
        .agg(F.max("ships").alias("ships"))
    )


def flag_offroute(reports: DataFrame, lanes: DataFrame, cfg: dict[str, Any]) -> DataFrame:
    """Off-route reports: checked reports whose cell has fewer than `min_ships` (`lane_ships`; 0 off the table).
    Works on a stream too (a join with the static lanes table)."""
    joined = checked_reports(reports, cfg).join(F.broadcast(lanes), ["cy", "cx"], "left")
    lane_ships = F.coalesce(F.col("ships"), F.lit(0))
    return (
        joined.withColumn("lane_ships", lane_ships)
        .where(F.col("lane_ships") < cfg["offroute"]["min_ships"])
        .drop("ships", "cy", "cx")
    )


def episodes(flagged: pd.DataFrame, gap_minutes: float) -> pd.DataFrame:
    """One alert per run of a ship's off-route reports with no more than `gap_minutes` between them."""
    if flagged.empty:
        return pd.DataFrame(columns=EPISODE_COLUMNS)
    f = flagged.sort_values(["mmsi", "ts"], kind="stable")
    new = (f["mmsi"] != f["mmsi"].shift()) | (f["ts"].diff() > pd.Timedelta(minutes=gap_minutes))
    return (
        f.assign(episode=new.cumsum())
        .groupby("episode")
        .agg(mmsi=("mmsi", "first"), name=("name", "first"), vessel_type=("vessel_type", "first"),
             start=("ts", "min"), end=("ts", "max"), reports=("ts", "size"), lat=("lat", "mean"),
             lon=("lon", "mean"), min_lane_ships=("lane_ships", "min"))
        .reset_index(drop=True)[EPISODE_COLUMNS]
    )
