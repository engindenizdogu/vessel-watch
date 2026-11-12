"""Shared forecast features, imported by the baseline, training, and serving so they use one code path.

The target is a count of gate crossings per time bin, built from the hourly crossings table (`make gates`).
Lags are looked up by timestamp, never by row position: the history is sampled in blocks of consecutive days,
and a row shift would quietly reach across a hole between blocks into a different month.
"""

from typing import Any

import pandas as pd


def target_series(hourly: pd.DataFrame, target: dict[str, Any], bin_hours: int, max_outage_minutes: int) -> pd.Series:
    """Crossings per `bin_hours` bin (UTC, aligned to midnight) at `target`'s gate and direction for its vessel
    groups, indexed by the bin's start. A bin is missing (NaN) if any of its hours has `max_outage_minutes` or
    more feed-outage minutes: a broken hour is unknown, not quiet."""
    rows = hourly[
        (hourly["gate"] == target["gate"])
        & (hourly["direction"] == target["direction"])
        & hourly["group"].isin(target["groups"])
    ]
    per_hour = rows.groupby("hour")["crossings"].sum()
    broken = rows.groupby("hour")["outage_minutes"].max() >= max_outage_minutes
    per_hour = per_hour.where(~broken).astype(float)
    return per_hour.groupby(per_hour.index.floor(f"{bin_hours}h")).sum(min_count=bin_hours).rename("y")


def lagged(y: pd.Series, hours: int) -> pd.Series:
    """`y`'s value `hours` earlier, looked up by timestamp; NaN where that time is missing or not in `y`."""
    return pd.Series(y.reindex(y.index - pd.Timedelta(hours=hours)).to_numpy(), index=y.index)
