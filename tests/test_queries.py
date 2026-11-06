from datetime import date, datetime, time

import pandas as pd

from app.queries import (
    available_days, gate_rates, latest_positions, load_shutdown_spots, local_to_utc, parquet_glob, vessels_per_hour,
)

TZ = "America/New_York"


def write_day(root, day: str, rows: list[tuple[str, int]]) -> None:
    """Write (ts, mmsi) rows as one Parquet file in a `date=` partition, like the Spark output."""
    folder = root / f"date={day}"
    folder.mkdir(parents=True)
    df = pd.DataFrame(rows, columns=["ts", "mmsi"]).assign(
        ts=lambda d: pd.to_datetime(d["ts"]), name="EXAMPLE", lat=40.6, lon=-74.0, sog=1.0, cog=0.0,
        heading=0, vessel_type=70, status=0,
    )
    df.to_parquet(folder / "part-0.parquet", index=False)


def test_local_to_utc_follows_daylight_saving():
    assert local_to_utc(date(2024, 7, 1), time(14), TZ) == datetime(2024, 7, 1, 18)  # EDT, UTC-4
    assert local_to_utc(date(2023, 12, 1), time(14), TZ) == datetime(2023, 12, 1, 19)  # EST, UTC-5


def test_available_days_reads_partition_names(tmp_path):
    write_day(tmp_path, "2024-07-02", [("2024-07-02 00:00:00", 1)])
    write_day(tmp_path, "2023-03-01", [("2023-03-01 00:00:00", 1)])
    assert available_days(tmp_path) == [date(2023, 3, 1), date(2024, 7, 2)]


def test_latest_positions_takes_latest_report_in_window_only(tmp_path):
    write_day(tmp_path, "2024-07-01", [
        ("2024-07-01 11:40:00", 1),  # before the 15-minute window
        ("2024-07-01 11:50:00", 2),
        ("2024-07-01 11:55:00", 2),  # vessel 2's latest
        ("2024-07-01 12:01:00", 3),  # after as_of
    ])
    df = latest_positions(parquet_glob(tmp_path), 15, datetime(2024, 7, 1, 12))
    assert df[["mmsi", "ts"]].values.tolist() == [[2, pd.Timestamp("2024-07-01 11:55:00")]]


def test_latest_positions_reads_across_midnight(tmp_path):
    write_day(tmp_path, "2024-07-01", [("2024-07-01 23:55:00", 1)])
    write_day(tmp_path, "2024-07-02", [("2024-07-02 00:05:00", 2)])
    df = latest_positions(parquet_glob(tmp_path), 15, datetime(2024, 7, 2, 0, 5))
    assert sorted(df["mmsi"]) == [1, 2]


def test_latest_positions_is_empty_without_parquet(tmp_path):
    assert latest_positions(parquet_glob(tmp_path), 15, datetime(2024, 7, 1)).empty


def test_vessels_per_hour_counts_distinct_vessels(tmp_path):
    write_day(tmp_path, "2024-07-01", [
        ("2024-07-01 10:00:00", 1), ("2024-07-01 10:30:00", 1), ("2024-07-01 10:45:00", 2),
        ("2024-07-01 11:10:00", 1),
        ("2024-07-01 12:00:00", 3),  # end is exclusive
    ])
    df = vessels_per_hour(parquet_glob(tmp_path), datetime(2024, 7, 1, 10), datetime(2024, 7, 1, 12))
    assert df["vessels"].tolist() == [2, 1]


def test_latest_positions_fills_columns_missing_from_older_files(tmp_path):
    write_day(tmp_path, "2024-07-01", [("2024-07-01 11:55:00", 1)])
    old = tmp_path / "date=2024-07-01" / "part-0.parquet"
    pd.read_parquet(old).drop(columns="name").to_parquet(old, index=False)
    df = latest_positions(parquet_glob(tmp_path), 15, datetime(2024, 7, 1, 12))
    assert list(df.columns)[:3] == ["mmsi", "name", "ts"] and df["name"].isna().all()


def test_load_shutdown_spots_reads_the_file_or_returns_empty(tmp_path):
    path = tmp_path / "shutdown_spots.parquet"
    assert load_shutdown_spots(path).empty
    pd.DataFrame({"lat": [40.7], "lon": [-74.01], "vessels": [2], "silences": [3], "median_silence_h": [9.0]}).to_parquet(path)
    assert load_shutdown_spots(path)[["vessels", "silences"]].values.tolist() == [[2, 3]]


def test_gate_rates_average_per_day(tmp_path):
    path = tmp_path / "hourly.parquet"
    assert gate_rates(path, ["cargo"]).empty
    hours = pd.to_datetime(["2024-07-01 03:00", "2024-07-02 05:00"])
    pd.DataFrame({"hour": hours.repeat(2), "gate": "g", "direction": "in", "group": ["cargo", "other"] * 2,
                  "crossings": [2, 1, 4, 3]}).to_parquet(path)
    r = gate_rates(path, ["cargo"]).iloc[0]
    assert (r.ships, r["all"]) == (3.0, 5.0)  # (2 + 4) / 2 days, (2 + 1 + 4 + 3) / 2 days
