"""Signal-gap rule, shared by the batch gap job and the streaming detector.

A Class A vessel (it reports a navigational status) that was reporting regularly goes silent longer than
its threshold: max(minimum for its state, multiplier x median of its recent report intervals). Vessels
last seen near the harbor box edge are skipped (they probably left), and no gap is raised past the end of
the data or across a feed outage (minutes when the whole feed went quiet). Gaps that start at a shutdown
spot, a place where vessels routinely switch AIS off, are labeled `known_spot`. Parameters are in the
`signal_gap` section of config.yaml.

The batch job runs `find_gaps` on each vessel's full history. The streaming job runs `track_gaps` per vessel
with `applyInPandasWithState`: the same per-report rule on new reports plus a short history kept in state, and
an event-time timeout that raises a gap when the watermark passes the alert time. One vessel's state cannot
see the rest of the feed, so the stream applies the feed-outage guard afterwards (`confirmed_gaps`), from its
own reports per minute.
"""

from collections.abc import Iterator
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
REPORT_COLUMNS = ["ts", "mmsi", "name", "lat", "lon", "sog", "vessel_type", "status"]
# Streaming state per vessel: its last report, the times of the reports before it that the rule still needs,
# and whether its current silence was already raised as an open gap. Times in epoch microseconds.
STATE_SCHEMA = (
    "ts_us long, name string, vessel_type int, status int, lat double, lon double, sog double, "
    "history_us array<long>, alerted boolean"
)
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


def feed_down_minutes(
    counts: pd.Series, days: list[date], fraction: float, until: datetime | None = None
) -> pd.DatetimeIndex:
    """Minutes on `days` with fewer reports than `fraction` x that day's median reports per minute.

    `counts` is reports per minute indexed by the minute's start (naive UTC); minutes missing from it count
    as zero. In a stream, minutes after `until` (the last minute counted so far) are left out. Returns the
    start of each outage minute.
    """
    minutes = pd.DatetimeIndex(np.concatenate([pd.date_range(d, periods=1440, freq="min") for d in sorted(days)]))
    if until is not None:
        minutes = minutes[minutes <= until]
    n = counts.reindex(minutes, fill_value=0)
    day_median = n.groupby(n.index.date).transform("median")
    return n.index[n < fraction * day_median]


def _seconds(values: Any) -> np.ndarray:
    return pd.to_datetime(values).to_numpy(dtype="datetime64[ns]").astype("int64") / 1e9


def report_thresholds(r: pd.DataFrame, secs: np.ndarray, cfg: dict[str, Any]) -> tuple[np.ndarray, ...]:
    """For each of one vessel's reports (sorted by time; `secs` are their epoch seconds): whether it is moving,
    its threshold in seconds, and whether a silence after it can be a gap (Class A, reporting regularly, away
    from the box edge)."""
    g = cfg["signal_gap"]
    median_prev = pd.Series(np.diff(secs, prepend=np.nan)).rolling(g["median_intervals"], min_periods=1).median()
    n_recent = np.arange(len(r)) - np.searchsorted(secs, secs - g["regular_window_seconds"], side="right") + 1
    moving = is_moving(r["sog"], g["moving_knots"])
    threshold = threshold_seconds(moving, median_prev.to_numpy(), g)
    lat, lon = r["lat"].to_numpy(dtype=float), r["lon"].to_numpy(dtype=float)
    eligible = (
        r["status"].notna().to_numpy()
        & (n_recent >= g["regular_reports"])
        & (edge_distance_km(lat, lon, cfg["harbor_bbox"]) >= g["edge_km"])
    )
    return moving, threshold, eligible


def spans_outage(last_s: np.ndarray, alert_s: np.ndarray, down_minutes: pd.DatetimeIndex) -> np.ndarray:
    """True where a feed outage minute starts after the last report's minute and before the alert time."""
    down = np.append(np.sort(_seconds(down_minutes)), np.inf)
    following = np.searchsorted(down[:-1], np.floor(last_s / 60) * 60, side="right")  # first outage after
    return down[following] < alert_s


def gap_rows(
    r: pd.DataFrame, is_gap: np.ndarray, moving: np.ndarray, threshold: np.ndarray, resumed: np.ndarray
) -> pd.DataFrame:
    """Gap rows (GAP_COLUMNS) for the reports flagged `is_gap` in one vessel's time-sorted reports `r`. Where
    the next report does not count as `resumed`, the gap is open (`resumed_ts`, `gap_s`, `moved_km` empty)."""
    lat, lon = r["lat"].to_numpy(dtype=float), r["lon"].to_numpy(dtype=float)
    moved = distance_km(lat, lon, np.append(lat[1:], np.nan), np.append(lon[1:], np.nan))
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


def find_gaps(
    reports: pd.DataFrame,
    cfg: dict[str, Any],
    coverage_end: dict[date, datetime],
    down_minutes: pd.DatetimeIndex = pd.DatetimeIndex([]),
) -> pd.DataFrame:
    """Signal gaps in one vessel's reports (REPORT_COLUMNS; any order).

    A gap is raised only if its threshold passes while the feed is still alive: before the end of the
    report's run of consecutive days (`coverage_end`, from `coverage_ends`) and before the first feed outage
    minute after the report's own minute (`down_minutes`, from `feed_down_minutes`). A gap whose next report
    is in a later run of days, or that has none, is open (`resumed_ts`, `gap_s`, `moved_km` empty). The next
    report still counts across an outage, so `gap_s` can include a few outage minutes.
    """
    r = reports.sort_values("ts", kind="stable").reset_index(drop=True)
    secs = _seconds(r["ts"])
    moving, threshold, eligible = report_thresholds(r, secs, cfg)
    alert = secs + threshold
    run_end = r["ts"].dt.date.map(coverage_end)
    if run_end.isna().any():
        raise ValueError("coverage_end is missing days that have reports")
    end = _seconds(run_end)
    next_secs = np.append(secs[1:], np.nan)
    resumed = next_secs <= end  # false when there is no next report or it is in a later run
    is_gap = (
        eligible
        & (alert <= end)
        & ~spans_outage(secs, alert, down_minutes)
        & (~resumed | (next_secs > alert))
    )
    return gap_rows(r, is_gap, moving, threshold, resumed)


def _state_reports(mmsi: int, state: tuple) -> pd.DataFrame:
    """A vessel's streaming state as reports: the earlier report times (other columns empty), then the last report."""
    ts_us, name, vessel_type, status, lat, lon, sog, history_us, _ = state
    n = len(history_us)

    def column(value: Any, dtype: str) -> pd.Series:
        return pd.Series([None] * n + [value], dtype=dtype)

    return pd.DataFrame({
        "ts": pd.to_datetime([*history_us, ts_us], unit="us"), "mmsi": mmsi, "name": column(name, "object"),
        "lat": column(lat, "float64"), "lon": column(lon, "float64"), "sog": column(sog, "float64"),
        "vessel_type": column(vessel_type, "Int32"), "status": column(status, "Int32"),
    })


def _py(value: Any) -> Any:
    """A pandas or NumPy scalar as a plain Python value for the state; missing values as None."""
    if pd.isna(value):
        return None
    return value.item() if hasattr(value, "item") else value


def track_gaps(key: tuple, batches: Iterator[pd.DataFrame], state: Any, cfg: dict[str, Any]) -> Iterator[pd.DataFrame]:
    """Streaming signal gaps for one vessel: the `applyInPandasWithState` function (state: STATE_SCHEMA,
    output: GAP_SCHEMA, event-time timeout).

    New reports raise the gaps that ended within them, with `resumed_ts` set. If the vessel then goes quiet,
    the timeout at its last report's alert time raises an open gap once the watermark passes it; the report
    that ends that silence raises it again, resumed, so the latest row per (mmsi, last_ts) is the current one.
    Reports at or before the last one already seen are dropped. The feed-outage guard and the shutdown-spot
    labels come afterwards (`confirmed_gaps`, `label_known_spots`).
    """
    (mmsi,) = key
    g = cfg["signal_gap"]
    old = _state_reports(mmsi, state.get) if state.exists else None
    if state.hasTimedOut:
        secs = _seconds(old["ts"])
        moving, threshold, _ = report_thresholds(old, secs, cfg)
        last = np.arange(len(old)) == len(old) - 1
        state.update(state.get[:-1] + (True,))
        yield gap_rows(old, last, moving, threshold, resumed=np.zeros(len(old), dtype=bool))
        return

    new = pd.concat(list(batches), ignore_index=True)[REPORT_COLUMNS]
    new = new.astype({"vessel_type": "Int32", "status": "Int32"}).sort_values("ts", kind="stable")
    new = new.drop_duplicates("ts")
    if old is not None:
        new = new[new["ts"] > old["ts"].iloc[-1]]
    r = pd.concat([df for df in (old, new) if df is not None and len(df)], ignore_index=True)
    secs = _seconds(r["ts"])
    moving, threshold, eligible = report_thresholds(r, secs, cfg)
    alert = secs + threshold
    next_secs = np.append(secs[1:], np.nan)
    resumed = ~np.isnan(next_secs)
    first = 0 if old is None else len(old) - 1  # the last report already seen, then the new ones
    is_gap = (np.arange(len(r)) >= first) & eligible & resumed & (next_secs > alert)

    # Keep what the rule needs for the next reports: the reports in the regular-reporting window and the
    # last `median_intervals` intervals.
    keep = (secs >= secs[-1] - g["regular_window_seconds"]) | (np.arange(len(r)) >= len(r) - g["median_intervals"] - 1)
    kept_us = r.loc[keep, "ts"].to_numpy(dtype="datetime64[us]").astype("int64").tolist()
    last = r.iloc[-1]
    alerted = old is not None and bool(state.get[-1]) and new.empty  # no new report: still the same silence
    state.update((
        kept_us[-1], _py(last["name"]), _py(last["vessel_type"]), _py(last["status"]), float(last["lat"]),
        float(last["lon"]), _py(last["sog"]), kept_us[:-1], alerted,
    ))
    if eligible[-1] and not alerted:  # a timeout must be set on every call, or the group has none
        state.setTimeoutTimestamp(max(int(np.ceil(alert[-1] * 1000)), state.getCurrentWatermarkMs() + 1))
    yield gap_rows(r, is_gap, moving, threshold, resumed)


def confirmed_gaps(events: pd.DataFrame, counts: pd.Series, cfg: dict[str, Any]) -> pd.DataFrame:
    """Gaps from the streaming detector's events (GAP_COLUMNS): the latest row per (mmsi, last_ts), minus gaps
    that span a feed outage, judged from the stream's reports per minute (`counts`, indexed by the minute's
    start, naive UTC) up to its last counted minute."""
    latest = (
        events.sort_values("resumed_ts", na_position="first", kind="stable")
        .drop_duplicates(["mmsi", "last_ts"], keep="last")
        .sort_values(["last_ts", "mmsi"])
        .reset_index(drop=True)
    )
    if counts.empty:
        return latest
    days = sorted(set(counts.index.date))
    down = feed_down_minutes(counts, days, cfg["signal_gap"]["outage_fraction"], until=counts.index.max())
    return latest[~spans_outage(_seconds(latest["last_ts"]), _seconds(latest["alert_ts"]), down)].reset_index(drop=True)


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
