"""Read-only DuckDB queries over date-partitioned position Parquet (live serving data or clean NOAA history)."""

from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd

POSITION_COLUMNS = ["mmsi", "name", "ts", "lat", "lon", "sog", "cog", "heading", "vessel_type", "status"]


def parquet_glob(folder: Path) -> str:
    """Glob for every Parquet file under a `date=`-partitioned folder."""
    return str(folder / "**" / "*.parquet")


def available_days(folder: Path) -> list[date]:
    """Dates of the `date=YYYY-MM-DD` partitions in `folder`, oldest first."""
    return sorted(date.fromisoformat(p.name.removeprefix("date=")) for p in folder.glob("date=*"))


def local_to_utc(day: date, t: time, tz: str) -> datetime:
    """Wall-clock time `t` on `day` in `tz`, as naive UTC like the stored timestamps."""
    return datetime.combine(day, t, tzinfo=ZoneInfo(tz)).astimezone(timezone.utc).replace(tzinfo=None)


def _query(sql: str, params: list[Any]) -> pd.DataFrame:
    # A connection per query: Streamlit serves each viewer from its own thread.
    with duckdb.connect() as con:
        return con.execute(sql, params).df()


def latest_positions(glob: str, minutes: int, as_of: datetime) -> pd.DataFrame:
    """Each vessel's latest report in the `minutes` up to `as_of` (naive UTC); empty if no Parquet exists yet."""
    start = as_of - timedelta(minutes=minutes)
    try:
        df = _query(
            f"""
            select * exclude (date)
            from read_parquet('{glob}', hive_partitioning = true, union_by_name = true)
            where date between ? and ? and ts > ? and ts <= ?
            qualify row_number() over (partition by mmsi order by ts desc) = 1
            """,
            [start.date(), as_of.date(), start, as_of],
        )
    except duckdb.IOException:  # no Parquet written yet
        return pd.DataFrame()
    return df.reindex(columns=POSITION_COLUMNS)  # files written before a column was added lack it


def vessels_per_hour(glob: str, start: datetime, end: datetime) -> pd.DataFrame:
    """Distinct vessels reporting in each hour from `start` to `end` (naive UTC, end exclusive)."""
    return _query(
        f"""
        select date_trunc('hour', ts) as hour, count(distinct mmsi) as vessels
        from read_parquet('{glob}', hive_partitioning = true)
        where date between ? and ? and ts >= ? and ts < ?
        group by 1 order by 1
        """,
        [start.date(), end.date(), start, end],
    )
