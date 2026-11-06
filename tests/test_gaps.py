from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from models.gaps import GAP_COLUMNS, GAP_SCHEMA, coverage_ends, find_gaps
from pipeline.config import load_config

CFG = load_config()  # thresholds: 10 min moving, 30 min stopped, 8x median interval, 5 reports in 15 min
T0 = datetime(2024, 7, 1, 12)
ONE_DAY = coverage_ends([date(2024, 7, 1)])


def track(minutes, sog=8.0, status=0, lat=40.65, lon=-74.05, mmsi=367000001, start=T0):
    """One vessel reporting at the given minutes after `start`, well inside the harbor box by default."""
    return pd.DataFrame({
        "ts": [start + timedelta(minutes=m) for m in minutes], "mmsi": mmsi, "name": "EXAMPLE",
        "lat": lat, "lon": lon, "sog": sog, "vessel_type": 70, "status": status,
    })


EVERY_MINUTE = list(range(11))  # 0..10: a regular reporter, last report at minute 10


def starts(gaps: pd.DataFrame) -> list[int]:
    """Minute (after T0) of the last report before each gap."""
    return [int((t - T0).total_seconds() // 60) for t in gaps["last_ts"]]


def test_moving_vessel_silent_past_ten_minutes_is_a_gap():
    gaps = find_gaps(track(EVERY_MINUTE + [25]), CFG, ONE_DAY)
    assert len(gaps) == 1
    g = gaps.iloc[0]
    assert (g.state, g.threshold_s, g.gap_s) == ("moving", 600, 900)
    assert g.last_ts == T0 + timedelta(minutes=10) and g.resumed_ts == T0 + timedelta(minutes=25)
    assert list(gaps.columns) == GAP_COLUMNS


def test_moving_vessel_silent_under_threshold_is_not_a_gap():
    # The 8-minute silence after minute 10 is not a gap; the track then ends while the vessel is still a
    # regular reporter, so the only gap is the open one after minute 18.
    gaps = find_gaps(track(EVERY_MINUTE + [18]), CFG, ONE_DAY)
    assert starts(gaps) == [18] and pd.isna(gaps.iloc[0].resumed_ts)


def test_stopped_vessel_uses_the_longer_minimum():
    assert find_gaps(track(EVERY_MINUTE + [30], sog=0.0), CFG, ONE_DAY).empty
    gaps = find_gaps(track(EVERY_MINUTE + [50], sog=0.0), CFG, ONE_DAY)
    assert (gaps.iloc[0].state, gaps.iloc[0].threshold_s) == ("stopped", 1800)


def test_missing_speed_counts_as_stopped():
    assert find_gaps(track(EVERY_MINUTE + [30], sog=np.nan), CFG, ONE_DAY).empty


def test_threshold_scales_with_the_vessels_usual_interval():
    every_two = list(range(0, 21, 2))  # median interval 120 s, so threshold = max(600, 8 x 120) = 960 s
    assert find_gaps(track(every_two + [35]), CFG, ONE_DAY).empty  # 15 min silence
    gaps = find_gaps(track(every_two + [37]), CFG, ONE_DAY)  # 17 min silence
    assert gaps.iloc[0].threshold_s == 960


def test_class_b_vessels_are_skipped():
    assert find_gaps(track(EVERY_MINUTE + [25], status=None), CFG, ONE_DAY).empty


def test_irregular_reporters_are_skipped():
    assert find_gaps(track([0, 5, 10, 25]), CFG, ONE_DAY).empty  # only 3 reports in the 15 min before


def test_vessels_last_seen_near_the_box_edge_are_skipped():
    assert find_gaps(track(EVERY_MINUTE + [25], lat=40.41), CFG, ONE_DAY).empty  # about 1 km from the edge


def test_vessel_never_seen_again_is_an_open_gap():
    gaps = find_gaps(track(EVERY_MINUTE), CFG, ONE_DAY)
    assert len(gaps) == 1 and pd.isna(gaps.iloc[0].resumed_ts) and pd.isna(gaps.iloc[0].gap_s)


def test_no_gap_when_the_data_ends_before_the_threshold():
    late = datetime(2024, 7, 1, 23, 50)  # threshold would pass at 00:10 the next day, after the data ends
    assert find_gaps(track(range(6), start=late), CFG, ONE_DAY).empty


def test_next_report_in_a_later_block_leaves_the_gap_open():
    ends = coverage_ends([date(2024, 7, 1), date(2024, 7, 10)])
    reports = pd.concat([track(EVERY_MINUTE), track([0], start=datetime(2024, 7, 10, 12))])
    gaps = find_gaps(reports, CFG, ends)  # the lone Jul 10 report is not a regular reporter, so no gap there
    assert len(gaps) == 1 and gaps.iloc[0].last_ts == T0 + timedelta(minutes=10)
    assert pd.isna(gaps.iloc[0].resumed_ts)  # not a 9-day gap: the data between the blocks is missing


def test_input_order_does_not_matter():
    reports = track(EVERY_MINUTE + [25])
    assert find_gaps(reports.sample(frac=1, random_state=0), CFG, ONE_DAY).equals(find_gaps(reports, CFG, ONE_DAY))


def test_coverage_ends_groups_consecutive_days():
    ends = coverage_ends([date(2024, 7, 2), date(2024, 7, 1), date(2024, 7, 5)])
    assert ends == {
        date(2024, 7, 1): datetime(2024, 7, 3), date(2024, 7, 2): datetime(2024, 7, 3),
        date(2024, 7, 5): datetime(2024, 7, 6),
    }


def test_missing_coverage_day_raises():
    with pytest.raises(ValueError):
        find_gaps(track(EVERY_MINUTE), CFG, {})


def test_runs_per_vessel_in_spark(spark):
    reports = pd.concat([track(EVERY_MINUTE + [25]), track(EVERY_MINUTE + [25], status=None, mmsi=367000002)])
    out = (
        spark.createDataFrame(reports).groupBy("mmsi")
        .applyInPandas(lambda v: find_gaps(v, CFG, ONE_DAY), schema=GAP_SCHEMA).collect()
    )
    assert [(r.mmsi, r.state, r.gap_s) for r in out] == [(367000001, "moving", 900.0)]
