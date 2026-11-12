import json
from datetime import date, datetime, timedelta, timezone

import pandas as pd
from pyspark.sql import functions as F

from ingest.replay import blocks, count_reports, missing_days, read_reports, to_message, wait_seconds
from pipeline.stream_job import RAW_SCHEMA, keep_run, parse_raw


def write_clean_day(root, day: str, rows: list[tuple[str, int]]) -> None:
    """Write (ts, mmsi) rows as a clean-history partition, with nulls where the cleaning nulled codes."""
    folder = root / f"date={day}"
    folder.mkdir(parents=True)
    df = pd.DataFrame(rows, columns=["ts", "mmsi"]).assign(
        ts=lambda d: pd.to_datetime(d["ts"]), name=None, lat=40.6, lon=-74.0, sog=1.5, cog=None,
        heading=pd.array([None] * len(rows), dtype="Int32"), vessel_type=pd.array([70] * len(rows), dtype="Int32"),
        status=pd.array([0] * len(rows), dtype="Int32"),
    )
    df.to_parquet(folder / "part-0.parquet", index=False)


def test_read_reports_orders_by_time_across_days_and_files(tmp_path):
    write_clean_day(tmp_path, "2024-07-01", [("2024-07-01 23:59:00", 2), ("2024-07-01 10:00:00", 1)])
    write_clean_day(tmp_path, "2024-07-02", [("2024-07-02 00:01:00", 1)])
    write_clean_day(tmp_path, "2024-07-03", [("2024-07-03 00:00:00", 3)])  # after the range
    reports = list(read_reports(tmp_path, date(2024, 7, 1), date(2024, 7, 2), batch_rows=1))
    assert [(r["ts"], r["mmsi"]) for r in reports] == [
        (datetime(2024, 7, 1, 10), 1), (datetime(2024, 7, 1, 23, 59), 2), (datetime(2024, 7, 2, 0, 1), 1),
    ]
    assert list(reports[0]) == RAW_SCHEMA.fieldNames()
    assert count_reports(tmp_path, date(2024, 7, 1), date(2024, 7, 2)) == 3


def test_message_keeps_nulls_and_formats_time_like_the_live_producer(tmp_path):
    write_clean_day(tmp_path, "2024-07-01", [("2024-07-01 10:00:00.5", 367000001)])
    message = to_message(next(read_reports(tmp_path, date(2024, 7, 1), date(2024, 7, 1))))
    assert json.loads(json.dumps(message)) == {
        "ts": "2024-07-01T10:00:00.500000Z", "mmsi": 367000001, "name": None, "lat": 40.6, "lon": -74.0,
        "sog": 1.5, "cog": None, "heading": None, "vessel_type": 70, "status": 0,
    }


def test_stream_job_parses_replay_messages(spark):
    message = to_message({"ts": datetime(2024, 7, 1, 10, 0, 0, 500000), "mmsi": 367000001, "name": "EXAMPLE",
                          "lat": 40.6, "lon": -74.0, "sog": 1.5, "cog": None, "heading": None,
                          "vessel_type": 70, "status": 0})
    parsed = parse_raw(spark.createDataFrame([(json.dumps(message).encode(),)], ["value"]))
    # collect() turns timestamps into local time; format in Spark's session time zone (UTC) instead
    row = parsed.withColumn("utc", F.date_format("ts", "yyyy-MM-dd HH:mm:ss.SSSSSS")).first()
    assert row.utc == "2024-07-01 10:00:00.500000"
    assert row.date == date(2024, 7, 1)
    assert (row.mmsi, row.heading, row.vessel_type) == (367000001, None, 70)


def test_wait_seconds_scales_event_time_by_speed():
    first = datetime(2024, 7, 1)
    assert wait_seconds(first + timedelta(minutes=10), first, elapsed_s=0, speed=10) == 60
    assert wait_seconds(first + timedelta(minutes=10), first, elapsed_s=75, speed=10) == -15  # behind: send now


def test_live_and_replay_runs_split_reports_by_age(spark):
    now = datetime.now(timezone.utc)
    rows = [(1, now - timedelta(hours=1)), (2, now - timedelta(days=9)), (3, datetime(2023, 3, 1, tzinfo=timezone.utc))]
    df = spark.createDataFrame(rows, ["mmsi", "ts"])
    assert sorted(r.mmsi for r in keep_run(df, "live", 8).collect()) == [1]
    assert sorted(r.mmsi for r in keep_run(df, "replay", 8).collect()) == [2, 3]


def test_ranges_must_be_consecutive_clean_days():
    days = [date(2023, 3, 8), date(2023, 3, 9), date(2023, 9, 14), date(2023, 9, 15), date(2024, 7, 1)]
    assert missing_days(days, date(2023, 3, 8), date(2023, 3, 9)) == []
    assert missing_days(days, date(2023, 3, 9), date(2023, 3, 11)) == [date(2023, 3, 10), date(2023, 3, 11)]
    assert blocks(days) == [
        (date(2023, 3, 8), date(2023, 3, 9)), (date(2023, 9, 14), date(2023, 9, 15)), (date(2024, 7, 1), date(2024, 7, 1)),
    ]
