from datetime import date

import pandas as pd

from pipeline.compact_job import compact_day, delete_files, prune_serving
from pipeline.config import load_config

CFG = load_config()
DAY = date(2026, 11, 5)


def write_reports(path, rows):
    """(minute, mmsi, lat) rows as one Parquet file, like the streaming sink writes (no date column)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({
        "mmsi": [m for _, m, _ in rows], "name": "EXAMPLE",
        # UTC-aware, so Spark reads a TIMESTAMP like the streaming sink writes (naive would be TIMESTAMP_NTZ)
        "ts": [pd.Timestamp(2026, 11, 5, 12, tz="UTC") + pd.Timedelta(minutes=t) for t, _, _ in rows],
        "lat": [lat for _, _, lat in rows], "lon": -74.0, "sog": 5.0, "cog": 90.0, "heading": 90,
        "vessel_type": 70, "status": 0,
    }).to_parquet(path, index=False, coerce_timestamps="us")  # Spark reads micro-, not nanoseconds
    return path


def test_compact_day_merges_with_history_and_drops_duplicates(spark, tmp_path):
    history = tmp_path / "history"
    first = [write_reports(tmp_path / "in" / "a.parquet", [(0, 367000001, 40.60), (1, 367000001, 40.6003)])]
    assert compact_day(spark, CFG, first, DAY, history) == 2
    # a later run: one late report and one repeat of a report already in history
    second = [write_reports(tmp_path / "in" / "b.parquet", [(1, 367000001, 40.6003), (2, 367000001, 40.6006)])]
    assert compact_day(spark, CFG, second, DAY, history) == 3
    out = pd.read_parquet(history)
    assert len(out) == 3 and len(list((history / f"date={DAY}").glob("*.parquet"))) == 1


def test_compact_day_drops_position_spikes(spark, tmp_path):
    rows = [(0, 367000001, 40.60), (1, 367000001, 40.90), (2, 367000001, 40.6006)]  # 18 nm away and back in 2 min
    files = [write_reports(tmp_path / "in" / "a.parquet", rows)]
    assert compact_day(spark, CFG, files, DAY, tmp_path / "history") == 2


def test_delete_files_removes_checksums_and_empty_folders(tmp_path):
    folder = tmp_path / "date=2026-11-05"
    folder.mkdir()
    f = folder / "part-0.parquet"
    f.write_text("x")
    (folder / ".part-0.parquet.crc").write_text("x")
    delete_files([f])
    assert not folder.exists()


def test_prune_serving_keeps_recent_days(tmp_path):
    for d in ("2026-11-03", "2026-11-05", "2026-11-06"):
        (tmp_path / f"date={d}").mkdir()
    assert prune_serving(tmp_path, date(2026, 11, 6), keep_days=2) == [date(2026, 11, 3)]
    assert sorted(p.name for p in tmp_path.iterdir()) == ["date=2026-11-05", "date=2026-11-06"]
