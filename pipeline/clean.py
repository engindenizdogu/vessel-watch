"""Shared cleaning rules for position reports in the shared schema
(ts, mmsi, lat, lon, sog, cog, heading, vessel_type, status).

Each step is a DataFrame -> DataFrame function. The batch job runs all steps and logs row counts;
the streaming job runs only the stateless ones, since the stateful ones need ordered per-vessel history.
"""

from typing import Any, Callable

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

Step = tuple[str, Callable[[DataFrame], DataFrame]]

EARTH_RADIUS_NM = 3440.065


def crop_to_bbox(df: DataFrame, box: dict[str, float]) -> DataFrame:
    """Keep reports inside the harbor box. This also drops out-of-range and unavailable (91/181) positions."""
    return df.where(
        F.col("lat").between(box["lat_min"], box["lat_max"])
        & F.col("lon").between(box["lon_min"], box["lon_max"])
    )


def drop_missing_keys(df: DataFrame) -> DataFrame:
    """Drop reports without a timestamp, MMSI, or position."""
    return df.dropna(subset=["ts", "mmsi", "lat", "lon"])


def drop_invalid_mmsi(df: DataFrame, mmsi_min: int, mmsi_max: int) -> DataFrame:
    """Keep 9-digit ship MMSIs (first digit 2-7); drops base stations, aids to navigation, handhelds, SAR."""
    return df.where(F.col("mmsi").between(mmsi_min, mmsi_max))


def null_unavailable(df: DataFrame) -> DataFrame:
    """Replace AIS "not available" codes with null: heading 511, SOG 102.3, COG 360 (and anything beyond)."""
    return (
        df.withColumn("heading", F.when(F.col("heading") < 360, F.col("heading")))
        .withColumn("sog", F.when(F.col("sog") < 102.3, F.col("sog")))
        .withColumn("cog", F.when(F.col("cog") < 360, F.col("cog")))
    )


def drop_duplicates(df: DataFrame) -> DataFrame:
    """Keep one report per (MMSI, timestamp)."""
    return df.dropDuplicates(["mmsi", "ts"])


def _knots_between(lat1: Column, lon1: Column, ts1: Column, lat2: Column, lon2: Column, ts2: Column) -> Column:
    """Implied speed in knots between two reports (haversine distance over elapsed time); null if no time passed."""
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = F.sin(dlat / 2) ** 2 + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2
    dist_nm = 2 * EARTH_RADIUS_NM * F.asin(F.sqrt(a))
    hours = (ts2.cast("double") - ts1.cast("double")) / 3600
    return F.when(hours > 0, dist_nm / hours)


def drop_position_spikes(df: DataFrame, max_knots: float) -> DataFrame:
    """Drop a report whose implied speed from both the previous and the next report exceeds `max_knots`.

    Requiring both sides removes an isolated bad fix without also dropping the good report after it.
    A vessel's first and last reports have one neighbour and are never dropped.
    """
    w = Window.partitionBy("mmsi").orderBy("ts")
    lat, lon, ts = F.col("lat"), F.col("lon"), F.col("ts")
    from_prev = _knots_between(F.lag(lat).over(w), F.lag(lon).over(w), F.lag(ts).over(w), lat, lon, ts)
    to_next = _knots_between(lat, lon, ts, F.lead(lat).over(w), F.lead(lon).over(w), F.lead(ts).over(w))
    # Null speeds (no neighbour, or no time elapsed) must not count as a spike.
    is_spike = F.coalesce((from_prev > max_knots) & (to_next > max_knots), F.lit(False))
    return df.withColumn("_spike", is_spike).where(~F.col("_spike")).drop("_spike")


def stateless_steps(cfg: dict[str, Any]) -> list[Step]:
    """Row-by-row cleaning steps, safe for streaming."""
    c = cfg["cleaning"]
    return [
        ("crop to harbor box", lambda df: crop_to_bbox(df, cfg["harbor_bbox"])),
        ("drop missing keys", drop_missing_keys),
        ("drop invalid MMSI", lambda df: drop_invalid_mmsi(df, c["mmsi_min"], c["mmsi_max"])),
        ("null unavailable codes", null_unavailable),
    ]


def batch_steps(cfg: dict[str, Any]) -> list[Step]:
    """All cleaning steps, in order, for historical data."""
    return stateless_steps(cfg) + [
        ("drop duplicates", drop_duplicates),
        ("drop position spikes", lambda df: drop_position_spikes(df, cfg["cleaning"]["max_implied_knots"])),
    ]
