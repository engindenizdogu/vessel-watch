"""Signal-gap rule, shared by the batch gap job and the streaming detector.

A Class A vessel (it reports a navigational status) that was reporting regularly goes silent longer than
its threshold: max(minimum for its state, multiplier x median of its recent report intervals). Vessels
last seen near the harbor box edge are skipped (they probably left), and no gap is raised past the end of
the data or across a feed outage (minutes when the whole feed went quiet). Gaps that start at a shutdown
spot, a place where vessels routinely switch AIS off, are labeled `known_spot`. Parameters are in the
`signal_gap` section of config.yaml.
"""

from datetime import date, datetime, time, timedelta
from typing import Any

import numpy as np
import pandas as pd

KM_PER_DEG_LAT = 111.0

GAP_SCHEMA = (
    "mmsi long, name string, vessel_type int, status int, state string, last_ts timestamp, lat double, "
    "lon double, sog double, threshold_s double, alert_ts timestamp, resumed_ts timestamp, gap_s double, "
    "moved_km double, date date"
)
GAP_COLUMNS = [c.split()[0] for c in GAP_SCHEMA.split(", ")]
LABELED_GAP_SCHEMA = GAP_SCHEMA + ", known_spot boolean"  # after label_known_spots
SPOT_COLUMNS = ["lat", "lon", "vessels", "silences", "median_silence_h"]


def is_moving(sog: pd.Series, moving_knots: float) -> np.ndarray:
    """True where speed is at least `moving_knots`; a missing speed counts as stopped."""
    return sog.to_numpy(dtype=float, na_value=np.nan) >= moving_knots


def threshold_seconds(moving: np.ndarray, median_interval_s: np.ndarray, g: dict[str, Any]) -> np.ndarray:
    """Silence allowed before a gap: max(state minimum, multiplier x median recent interval)."""
    minimum = np.where(moving, g["min_moving_seconds"], g["min_stopped_seconds"])
    return np.maximum(minimum, g["interval_multiplier"] * np.nan_to_num(median_interval_s, nan=0.0))


def edge_distance_km(lat: np.ndarray, lon: np.ndarray, box: dict[str, float]) -> np.ndarray:
    """Distance from each position to the nearest edge of the box, in km (flat-earth approximation)."""
    dlat = np.minimum(lat - box["lat_min"], box["lat_max"] - lat) * KM_PER_DEG_LAT
    dlon = np.minimum(lon - box["lon_min"], box["lon_max"] - lon) * KM_PER_DEG_LAT * np.cos(np.radians(lat))
    return np.minimum(dlat, dlon)


def distance_km(lat1: np.ndarray, lon1: np.ndarray, lat2: np.ndarray, lon2: np.ndarray) -> np.ndarray:
    """Distance between position pairs, in km (flat-earth approximation, fine within the harbor box)."""
    dlon = (lon2 - lon1) * np.cos(np.radians((lat1 + lat2) / 2))
    return np.hypot(lat2 - lat1, dlon) * KM_PER_DEG_LAT


def coverage_ends(days: list[date]) -> dict[date, datetime]:
    """Map each day with data to the end (exclusive, naive UTC) of its run of consecutive days."""
    days = sorted(set(days))
    ends: dict[date, datetime] = {}
    run_start = 0
    for i, d in enumerate(days):
        if i + 1 == len(days) or days[i + 1] != d + timedelta(days=1):
            end = datetime.combine(d + timedelta(days=1), time())
            ends.update({r: end for r in days[run_start:i + 1]})
            run_start = i + 1
    return ends


def feed_down_minutes(counts: pd.Series, days: list[date], fraction: float) -> pd.DatetimeIndex:
    """Minutes on `days` with fewer reports than `fraction` x that day's median reports per minute.

    `counts` is reports per minute indexed by the minute's start (naive UTC); minutes missing from it count
    as zero. Returns the start of each outage minute.
    """
    minutes = pd.DatetimeIndex(np.concatenate([pd.date_range(d, periods=1440, freq="min") for d in sorted(days)]))
    n = counts.reindex(minutes, fill_value=0)
    day_median = n.groupby(n.index.date).transform("median")
    return n.index[n < fraction * day_median]


def _seconds(values: Any) -> np.ndarray:
    return pd.to_datetime(values).to_numpy(dtype="datetime64[ns]").astype("int64") / 1e9


def find_gaps(
    reports: pd.DataFrame,
    cfg: dict[str, Any],
    coverage_end: dict[date, datetime],
    down_minutes: pd.DatetimeIndex = pd.DatetimeIndex([]),
) -> pd.DataFrame:
    """Signal gaps in one vessel's reports (ts, mmsi, name, lat, lon, sog, vessel_type, status; any order).

    A gap is raised only if its threshold passes while the feed is still alive: before the end of the
    report's run of consecutive days (`coverage_end`, from `coverage_ends`) and before the first feed outage
    minute after the report's own minute (`down_minutes`, from `feed_down_minutes`). A gap whose next report
    is in a later run of days, or that has none, is open (`resumed_ts`, `gap_s`, `moved_km` empty). The next
    report still counts across an outage, so `gap_s` can include a few outage minutes.
    """
    g = cfg["signal_gap"]
    r = reports.sort_values("ts", kind="stable").reset_index(drop=True)
    secs = _seconds(r["ts"])
    median_prev = pd.Series(np.diff(secs, prepend=np.nan)).rolling(g["median_intervals"], min_periods=1).median()
    n_recent = np.arange(len(r)) - np.searchsorted(secs, secs - g["regular_window_seconds"], side="right") + 1

    moving = is_moving(r["sog"], g["moving_knots"])
    threshold = threshold_seconds(moving, median_prev.to_numpy(), g)
    alert = secs + threshold
    run_end = r["ts"].dt.date.map(coverage_end)
    if run_end.isna().any():
        raise ValueError("coverage_end is missing days that have reports")
    end = _seconds(run_end)
    down = np.append(np.sort(_seconds(down_minutes)), np.inf)
    following = np.searchsorted(down[:-1], np.floor(secs / 60) * 60, side="right")  # first outage after this minute
    alive_until = np.minimum(end, down[following])
    next_secs = np.append(secs[1:], np.nan)
    resumed = next_secs <= end  # false when there is no next report or it is in a later run

    lat, lon = r["lat"].to_numpy(dtype=float), r["lon"].to_numpy(dtype=float)
    moved = distance_km(lat, lon, np.append(lat[1:], np.nan), np.append(lon[1:], np.nan))
    is_gap = (
        r["status"].notna().to_numpy()
        & (n_recent >= g["regular_reports"])
        & (edge_distance_km(lat, lon, cfg["harbor_bbox"]) >= g["edge_km"])
        & (alert <= alive_until)
        & (~resumed | (next_secs > alert))
    )

    out = r.loc[is_gap, ["mmsi", "name", "vessel_type", "status", "lat", "lon", "sog"]].reset_index(drop=True)
    out["state"] = np.where(moving[is_gap], "moving", "stopped")
    out["last_ts"] = r.loc[is_gap, "ts"].to_numpy()
    out["threshold_s"] = threshold[is_gap]
    out["alert_ts"] = out["last_ts"] + pd.to_timedelta(out["threshold_s"], unit="s")
    next_ts = r["ts"].shift(-1)[is_gap].to_numpy()
    out["resumed_ts"] = pd.Series(next_ts).where(resumed[is_gap])
    out["gap_s"] = (out["resumed_ts"] - out["last_ts"]).dt.total_seconds()
    out["moved_km"] = np.where(resumed[is_gap], moved[is_gap], np.nan)
    out["date"] = out["last_ts"].dt.date
    return out.astype({"mmsi": "int64", "vessel_type": "Int32", "status": "Int32"})[GAP_COLUMNS]


def cell_index(lat: Any, lon: Any, deg: float) -> tuple[np.ndarray, np.ndarray]:
    """Grid cell of each position: latitude and longitude divided by `deg`, rounded to integers."""
    lat, lon = np.asarray(lat, dtype=float), np.asarray(lon, dtype=float)
    return np.round(lat / deg).astype(np.int64), np.round(lon / deg).astype(np.int64)


def shutdown_spots(gaps: pd.DataFrame, g: dict[str, Any]) -> pd.DataFrame:
    """Grid cells where AIS is routinely switched off (berths, docks), learned from past gaps.

    A cell is a spot when at least `spot_min_vessels` vessels had a gap starting there that lasted
    `spot_min_silence_seconds` or more and ended within `spot_max_move_km` of where it started. Returns the
    cell centers with the number of vessels, the number of such silences, and their median length in hours.
    """
    deg = g["spot_cell_deg"]
    quiet = gaps[(gaps["gap_s"] >= g["spot_min_silence_seconds"]) & (gaps["moved_km"] <= g["spot_max_move_km"])]
    cy, cx = cell_index(quiet["lat"], quiet["lon"], deg)
    spots = (
        quiet.assign(cy=cy, cx=cx, silence_h=quiet["gap_s"] / 3600)
        .groupby(["cy", "cx"], as_index=False)
        .agg(vessels=("mmsi", "nunique"), silences=("mmsi", "size"), median_silence_h=("silence_h", "median"))
    )
    spots = spots[spots["vessels"] >= g["spot_min_vessels"]]
    return spots.assign(lat=spots["cy"] * deg, lon=spots["cx"] * deg)[SPOT_COLUMNS].reset_index(drop=True)


def label_known_spots(gaps: pd.DataFrame, spots: pd.DataFrame, g: dict[str, Any]) -> pd.DataFrame:
    """`gaps` plus `known_spot`: True where the gap's last position is in a shutdown spot's grid cell."""
    deg = g["spot_cell_deg"]
    known = set(zip(*cell_index(spots["lat"], spots["lon"], deg)))
    cells = zip(*cell_index(gaps["lat"], gaps["lon"], deg))
    return gaps.assign(known_spot=np.array([c in known for c in cells], dtype=bool))
