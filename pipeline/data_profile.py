"""Profile the clean historical Parquet with DuckDB and write docs/DATA_PROFILE.md.

The numbers calibrate the draft thresholds in the plan: signal-gap time per status (report intervals)
and the position-spike speed (implied speed between consecutive reports).
"""

from datetime import datetime, timezone

import duckdb
import pandas as pd

from pipeline.config import ROOT, load_config

OUT_PATH = ROOT / "docs" / "DATA_PROFILE.md"

# AIS navigational status codes seen in the harbor; the rest are grouped as "other".
STATUS_SQL = """case status
    when 0 then '0 under way (engine)' when 1 then '1 at anchor' when 3 then '3 restricted manoeuvrability'
    when 5 then '5 moored' when 7 then '7 fishing' when 8 then '8 under way (sailing)'
    when 15 then '15 undefined' else coalesce('other (' || status || ')', 'null') end"""

# Vessel type groups from the AIS type code (tens digit, plus the common special codes).
TYPE_SQL = """case
    when vessel_type is null or vessel_type = 0 then 'unknown'
    when vessel_type in (31, 32, 52) then 'tug / towing'
    when vessel_type = 37 then 'pleasure craft'
    when vessel_type = 36 then 'sailing'
    when vessel_type between 60 and 69 then 'passenger'
    when vessel_type between 70 and 79 then 'cargo'
    when vessel_type between 80 and 89 then 'tanker'
    else 'other' end"""

# Consecutive reports per vessel within a day: interval since the previous report and implied speed.
PAIRS_SQL = """
create view pairs as
select *, epoch(ts) - epoch(prev_ts) as interval_s,
    case when epoch(ts) > epoch(prev_ts) then
        2 * 3440.065 * asin(sqrt(
            pow(sin(radians(lat - prev_lat) / 2), 2)
            + cos(radians(prev_lat)) * cos(radians(lat)) * pow(sin(radians(lon - prev_lon) / 2), 2)
        )) / ((epoch(ts) - epoch(prev_ts)) / 3600)
    end as knots
from (
    select mmsi, ts, lat, lon,
        lag(ts) over w as prev_ts, lag(lat) over w as prev_lat, lag(lon) over w as prev_lon,
        lag(status) over w as prev_status
    from clean
    window w as (partition by mmsi, date order by ts)
)
where prev_ts is not null
"""

QUERIES = {
    "Overview": """
        select date, count(*) as reports, count(distinct mmsi) as vessels,
            round(count(*) / count(distinct mmsi)) as reports_per_vessel
        from clean group by date order by date""",
    "Field completeness (% non-null after cleaning)": """
        select date, round(100 * count(sog) / count(*), 1) as sog, round(100 * count(cog) / count(*), 1) as cog,
            round(100 * count(heading) / count(*), 1) as heading, round(100 * count(status) / count(*), 1) as status,
            round(100 * count(vessel_type) / count(*), 1) as vessel_type
        from clean group by date order by date""",
    "Traffic per hour in the harbor box": """
        with h as (
            select date, date_trunc('hour', ts) as hour, count(*) as reports, count(distinct mmsi) as vessels
            from clean group by all
        )
        select date, count(*) as hours, min(vessels) as min_vessels, median(vessels) as median_vessels,
            max(vessels) as max_vessels, min(reports) as min_reports, median(reports) as median_reports,
            max(reports) as max_reports
        from h group by date order by date""",
    "Vessels by type": f"""
        select {TYPE_SQL} as type_group, count(distinct mmsi) as vessels, count(*) as reports
        from clean group by all order by vessels desc""",
    "Report intervals by status of the previous report (seconds)": f"""
        select {STATUS_SQL.replace('status', 'prev_status')} as status, count(*) as intervals,
            round(quantile_cont(interval_s, 0.5)) as p50, round(quantile_cont(interval_s, 0.9)) as p90,
            round(quantile_cont(interval_s, 0.95)) as p95, round(quantile_cont(interval_s, 0.99)) as p99,
            round(100 * avg((interval_s > 600)::int), 2) as pct_over_10min,
            round(100 * avg((interval_s > 1800)::int), 2) as pct_over_30min
        from pairs group by all order by intervals desc""",
    "Implied speed between consecutive reports (knots)": """
        select count(knots) as pairs, round(quantile_cont(knots, 0.5), 1) as p50,
            round(quantile_cont(knots, 0.99), 1) as p99, round(quantile_cont(knots, 0.999), 1) as p99_9,
            round(max(knots), 1) as max, count(*) filter (where knots > 30) as over_30,
            count(*) filter (where knots > 50) as over_50
        from pairs""",
}


def md_table(df: pd.DataFrame) -> str:
    """Render a DataFrame as a Markdown table."""
    rows = [list(df.columns), [":--"] * len(df.columns)] + df.astype(str).values.tolist()
    return "\n".join("| " + " | ".join(map(str, r)) + " |" for r in rows)


def main() -> None:
    cfg = load_config()
    clean_glob = cfg["paths"]["clean"] / "**" / "*.parquet"
    con = duckdb.connect()
    con.execute(f"create view clean as select * from read_parquet('{clean_glob}', hive_partitioning = true)")
    con.execute(PAIRS_SQL)

    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    sections = [
        "# Data Profile",
        f"Generated by `make profile` on {stamp} from `data/clean/` (NOAA, harbor box). Do not edit by hand.",
        "Intervals and speeds use consecutive reports of the same vessel on the same day. An interval also "
        "includes time spent outside the harbor box, so long intervals are an upper bound on real signal gaps.",
    ]
    for title, sql in QUERIES.items():
        df = con.execute(sql).df()
        print(f"\n{title}\n{df.to_string(index=False)}")
        sections.append(f"## {title}\n\n{md_table(df)}")

    OUT_PATH.write_text("\n\n".join(sections) + "\n")
    print(f"\nwrote {OUT_PATH.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
