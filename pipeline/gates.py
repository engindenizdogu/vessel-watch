"""Gate crossings: vessels entering and leaving the harbor, shared by the batch gate job and (later) streaming.

A crossing is a pair of consecutive reports of one vessel, at most `gates.max_gap_minutes` apart, whose
straight track crosses a gate line. It is "in" when the second report is on the harbor side of the line
(the side of the gate's `inside` point), else "out", and is timed at that second report. Gates and vessel
groups are in config.yaml (`gates`, `vessel_groups`).
"""

from datetime import date
from typing import Any

import numpy as np
import pandas as pd

CROSSING_SCHEMA = (
    "mmsi long, name string, vessel_type int, gate string, direction string, ts timestamp, lat double, "
    "lon double, date date"
)
CROSSING_COLUMNS = [c.split()[0] for c in CROSSING_SCHEMA.split(", ")]
OTHER = "other"


def _side(ax: Any, ay: Any, bx: Any, by: Any, px: Any, py: Any) -> Any:
    """Which side of the line a->b point p is on: positive left, negative right, zero on the line."""
    return (bx - ax) * (py - ay) - (by - ay) * (px - ax)


def find_crossings(reports: pd.DataFrame, g: dict[str, Any]) -> pd.DataFrame:
    """Gate crossings in one vessel's reports (ts, mmsi, name, vessel_type, lat, lon; any order); `g` is `gates`."""
    r = reports.sort_values("ts", kind="stable").reset_index(drop=True)
    x, y = r["lon"].to_numpy(dtype=float), r["lat"].to_numpy(dtype=float)
    px, py = np.roll(x, 1), np.roll(y, 1)  # previous report; row 0 is excluded by `joined`
    joined = r["ts"].diff().le(pd.Timedelta(minutes=g["max_gap_minutes"])).to_numpy()
    found = []
    for gate in g["lines"]:
        (ax, ay), (bx, by) = gate["line"]
        before, after = _side(ax, ay, bx, by, px, py), _side(ax, ay, bx, by, x, y)
        # The two reports are on opposite sides of the gate line, and the gate's ends on opposite sides of the track.
        hit = joined & (before * after < 0) & (_side(px, py, x, y, ax, ay) * _side(px, py, x, y, bx, by) < 0)
        inside = np.sign(_side(ax, ay, bx, by, *gate["inside"]))
        found.append(r[hit].assign(gate=gate["name"], direction=np.where(np.sign(after[hit]) == inside, "in", "out")))
    out = pd.concat(found).assign(date=lambda d: d["ts"].dt.date)
    return out.astype({"mmsi": "int64", "vessel_type": "Int32"})[CROSSING_COLUMNS].reset_index(drop=True)


def vessel_group(vessel_type: pd.Series, groups: dict[str, list[list[int]]]) -> pd.Series:
    """Group name for each AIS ship type code (`vessel_groups` in config.yaml); "other" if none matches or empty."""
    out = pd.Series(OTHER, index=vessel_type.index)
    for name, ranges in groups.items():
        for lo, hi in ranges:
            out[vessel_type.between(lo, hi).fillna(False).astype(bool)] = name
    return out


def hourly_counts(
    crossings: pd.DataFrame, days: list[date], gates: list[str], groups: dict[str, list[list[int]]]
) -> pd.DataFrame:
    """Crossings per UTC hour, gate, direction, and vessel group over every hour of `days`, zeros included."""
    hours = pd.DatetimeIndex([h for d in sorted(days) for h in pd.date_range(d, periods=24, freq="h")])
    keys = ["hour", "gate", "direction", "group"]
    grid = pd.MultiIndex.from_product([hours, gates, ["in", "out"], [*groups, OTHER]], names=keys)
    counts = crossings.assign(
        hour=crossings["ts"].dt.floor("h"), group=vessel_group(crossings["vessel_type"], groups)
    ).groupby(keys).size()
    return counts.reindex(grid, fill_value=0).rename("crossings").reset_index()
