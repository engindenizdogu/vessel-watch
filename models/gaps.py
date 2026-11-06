"""Signal-gap rule, shared by the batch gap job and the streaming detector.

A Class A vessel (it reports a navigational status) that was reporting regularly goes silent longer than
its threshold: max(minimum for its state, multiplier x median of its recent report intervals). Vessels
last seen near the harbor box edge are skipped (they probably left), and no gap is raised past the end of
the data. Parameters are in the `signal_gap` section of config.yaml.
"""

from datetime import date, datetime, time, timedelta
from typing import Any

import numpy as np
import pandas as pd

KM_PER_DEG_LAT = 111.0

GAP_SCHEMA = (
    "mmsi long, name string, vessel_type int, status int, state string, last_ts timestamp, lat double, "
    "lon double, sog double, threshold_s double, alert_ts timestamp, resumed_ts timestamp, gap_s double, "
    "date date"
)
GAP_COLUMNS = [c.split()[0] for c in GAP_SCHEMA.split(", ")]


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


def _seconds(values: Any) -> np.ndarray:
    return pd.to_datetime(values).to_numpy(dtype="datetime64[ns]").astype("int64") / 1e9


def find_gaps(reports: pd.DataFrame, cfg: dict[str, Any], coverage_end: dict[date, datetime]) -> pd.DataFrame:
    """Signal gaps in one vessel's reports (ts, mmsi, name, lat, lon, sog, vessel_type, status; any order).

    `coverage_end` (from `coverage_ends`) bounds each report's run of data: a gap whose next report comes
    after the run ends is open (`resumed_ts` and `gap_s` empty), and none is raised if the threshold would
    pass after the run ends.
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
    next_secs = np.append(secs[1:], np.nan)
    resumed = next_secs <= end  # false when there is no next report or it is in a later run

    lat, lon = r["lat"].to_numpy(dtype=float), r["lon"].to_numpy(dtype=float)
    is_gap = (
        r["status"].notna().to_numpy()
        & (n_recent >= g["regular_reports"])
        & (edge_distance_km(lat, lon, cfg["harbor_bbox"]) >= g["edge_km"])
        & np.where(resumed, next_secs > alert, alert <= end)
    )

    out = r.loc[is_gap, ["mmsi", "name", "vessel_type", "status", "lat", "lon", "sog"]].reset_index(drop=True)
    out["state"] = np.where(moving[is_gap], "moving", "stopped")
    out["last_ts"] = r.loc[is_gap, "ts"].to_numpy()
    out["threshold_s"] = threshold[is_gap]
    out["alert_ts"] = out["last_ts"] + pd.to_timedelta(out["threshold_s"], unit="s")
    next_ts = r["ts"].shift(-1)[is_gap].to_numpy()
    out["resumed_ts"] = pd.Series(next_ts).where(resumed[is_gap])
    out["gap_s"] = (out["resumed_ts"] - out["last_ts"]).dt.total_seconds()
    out["date"] = out["last_ts"].dt.date
    return out.astype({"mmsi": "int64", "vessel_type": "Int32", "status": "Int32"})[GAP_COLUMNS]
