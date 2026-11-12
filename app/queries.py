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


def latest_report_time(folder: Path) -> datetime | None:
    """Time (naive UTC) of the newest report in a `date=`-partitioned folder; None if it has none yet."""
    days = available_days(folder)
    if not days:
        return None
    try:
        ts = _query(f"select max(ts) as ts from read_parquet('{folder / f'date={days[-1]}'}/*.parquet')", [])["ts"].iloc[0]
    except duckdb.IOException:  # partition folder without files yet
        return None
    return None if pd.isna(ts) else ts.to_pydatetime()


def load_shutdown_spots(path: Path) -> pd.DataFrame:
    """Shutdown spots written by the gap job (lat, lon, vessels, silences, median_silence_h); empty if none yet."""
    try:
        return _query(f"select * from read_parquet('{path}')", [])
    except duckdb.IOException:
        return pd.DataFrame()


def gate_rates(path: Path, groups: list[str]) -> pd.DataFrame:
    """Average crossings per day by gate and direction: `ships` (vessel `groups`) and `all`; empty if none yet."""
    listed = ", ".join(f"'{g}'" for g in groups)
    try:
        return _query(
            f"""
            select gate, direction,
                sum(crossings) filter (where "group" in ({listed})) / count(distinct cast(hour as date)) as ships,
                sum(crossings) / count(distinct cast(hour as date)) as "all"
            from read_parquet('{path}') group by 1, 2
            """,
            [],
        )
    except duckdb.IOException:
        return pd.DataFrame()


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
