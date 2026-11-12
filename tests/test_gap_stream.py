from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from models.gaps import (
    GAP_COLUMNS, confirmed_gaps, coverage_ends, feed_down_minutes, find_gaps, track_gaps,
)
from pipeline.config import load_config

CFG = load_config()
DAY = date(2024, 7, 1)
T0 = datetime(2024, 7, 1, 12)
END_MS = pd.Timestamp("2024-07-02").value // 10**6
KEY = ["mmsi", "last_ts", "alert_ts", "resumed_ts"]


class FakeState:
    """The parts of PySpark's GroupState that track_gaps uses."""

    def __init__(self) -> None:
        self.value: tuple | None = None
        self.hasTimedOut = False
        self.watermark_ms = 0
        self.timeout_ms: int | None = None

    @property
    def exists(self) -> bool:
        return self.value is not None

    @property
    def get(self) -> tuple:
        return tuple(self.value)

    def update(self, value: tuple) -> None:
        self.value = tuple(value)

    def setTimeoutTimestamp(self, ms: int) -> None:
        assert ms > self.watermark_ms, "Spark rejects a timeout behind the watermark"
        self.timeout_ms = ms

    def getCurrentWatermarkMs(self) -> int:
        return self.watermark_ms


def run_stream(reports: pd.DataFrame, batch_minutes: float, delay_s: float = 120) -> pd.DataFrame:
    """Feed `reports` to track_gaps the way Spark would: micro-batches of `batch_minutes` of event time, a
    watermark trailing the newest report by `delay_s`, timeouts fired once the watermark passes them, and
    finally a watermark at the end of the day (the rest of the feed kept reporting)."""
    states: dict[int, FakeState] = {}
    events: list[pd.DataFrame] = []
    watermark = 0

    def call(mmsi: int, rows: pd.DataFrame, timed_out: bool) -> None:
        state = states.setdefault(mmsi, FakeState())
        state.hasTimedOut, state.watermark_ms, state.timeout_ms = timed_out, watermark, None  # reset per call
        events.extend(track_gaps((mmsi,), iter([rows]), state, CFG))

    def fire_timeouts(busy: set[int]) -> None:
        for mmsi, state in states.items():
            if mmsi not in busy and state.timeout_ms is not None and state.timeout_ms < watermark:
                call(mmsi, reports.iloc[:0], timed_out=True)

    batch = ((reports["ts"] - reports["ts"].min()) / pd.Timedelta(minutes=batch_minutes)).astype(int)
    for _, rows in reports.groupby(batch):
        rows = rows.sample(frac=1, random_state=1)  # Spark does not keep the order within a batch
        for mmsi, vessel in rows.groupby("mmsi"):
            call(mmsi, vessel, timed_out=False)
        fire_timeouts(set(rows["mmsi"]))
        watermark = max(watermark, int(rows["ts"].max().value // 10**6 - delay_s * 1000))
    watermark = END_MS
    fire_timeouts(set())
    return pd.concat(events, ignore_index=True)


def track(minutes, sog=8.0, status=0, lat=40.65, mmsi=367000001):
    return pd.DataFrame({
        "ts": [T0 + timedelta(minutes=m) for m in minutes], "mmsi": mmsi, "name": "EXAMPLE", "lat": lat,
        "lon": -74.05, "sog": sog, "vessel_type": 70, "status": status,
    })


def random_track(mmsi: int, seed: int) -> pd.DataFrame:
    """A vessel reporting about once a minute with occasional long silences and changes of speed."""
    rng = np.random.default_rng(seed)
    gaps = np.where(rng.random(400) < 0.03, rng.uniform(600, 4000, 400), rng.exponential(60, 400))
    ts = T0 - timedelta(hours=6) + pd.to_timedelta(np.cumsum(gaps).round(), unit="s")
    sog = np.where(rng.random(400) < 0.5, 0.2, 9.0)
    return pd.DataFrame({
        "ts": ts, "mmsi": mmsi, "name": f"V{mmsi}", "lat": 40.65, "lon": -74.05, "sog": sog,
        "vessel_type": 70, "status": 0,
    }).query("ts < '2024-07-02'").drop_duplicates("ts")  # like clean data: one report per vessel and time


SCENARIO = pd.concat([
    track(list(range(11)) + list(range(25, 40))),                   # moving: a gap after minute 10, then open
    track(list(range(11)) + [50, 51], sog=0.0, mmsi=367000002),      # stopped: 40 min silence, then open
    track(list(range(11)) + [30], status=None, mmsi=367000003),      # Class B: never a gap
    track([0, 5, 10, 25], mmsi=367000004),                           # irregular: never a gap
    random_track(367000005, seed=1), random_track(367000006, seed=2),
], ignore_index=True)


@pytest.mark.parametrize("batch_minutes", [1, 7, 60, 24 * 60])
def test_stream_reproduces_batch_gaps(batch_minutes):
    batch = pd.concat([find_gaps(v, CFG, coverage_ends([DAY])) for _, v in SCENARIO.groupby("mmsi")])
    stream = confirmed_gaps(run_stream(SCENARIO, batch_minutes), pd.Series(dtype=float), CFG)
    assert len(batch) > 10
    pd.testing.assert_frame_equal(
        stream[GAP_COLUMNS].sort_values(KEY).reset_index(drop=True),
        batch.sort_values(KEY).reset_index(drop=True),
        check_dtype=False,
    )


def test_open_gap_is_raised_at_its_alert_time_then_closed_by_the_next_report():
    others = track(range(60), status=None, mmsi=367000009)  # the rest of the feed keeps reporting (Class B: no gaps)
    events = run_stream(pd.concat([track(list(range(11)) + [40]), others]), batch_minutes=1)
    # Raised open once the watermark passed minute 20, then again when minute 40 resumed it. Minute 40 itself is
    # not a regular reporter, so no gap follows it.
    assert events["last_ts"].tolist() == [T0 + timedelta(minutes=10)] * 2
    assert events["resumed_ts"].isna().tolist() == [True, False]
    gaps = confirmed_gaps(events, pd.Series(dtype=float), CFG)
    assert len(gaps) == 1 and gaps.iloc[0].gap_s == 1800


def test_reports_at_or_before_the_last_one_seen_are_dropped():
    reports = track(list(range(11)) + [25])
    late = reports.iloc[[3]]  # minute 3 again, after minute 25 was seen
    events = run_stream(pd.concat([reports, late.assign(ts=late["ts"])]), batch_minutes=24 * 60)
    assert confirmed_gaps(events, pd.Series(dtype=float), CFG)["gap_s"].tolist() == [900]


def minute_counts(n_minutes: int, quiet: list[int]) -> pd.Series:
    """100 reports per minute for the first `n_minutes` minutes of the day, 5 in the `quiet` ones."""
    minutes = pd.date_range(DAY, periods=n_minutes, freq="min")
    return pd.Series(np.where(np.isin(np.arange(n_minutes), quiet), 5, 100), index=minutes)


def test_confirmed_gaps_drops_gaps_that_span_a_feed_outage():
    events = run_stream(track(list(range(11)) + [25]), batch_minutes=1)
    noon = 12 * 60  # T0
    assert len(confirmed_gaps(events, minute_counts(noon + 30, quiet=[]), CFG)) == 1
    assert confirmed_gaps(events, minute_counts(noon + 30, quiet=[noon + 15]), CFG).empty


def test_feed_down_minutes_ignores_minutes_not_counted_yet():
    counts = minute_counts(600, quiet=[100])  # the stream has counted the day up to 09:59
    assert list(feed_down_minutes(counts, [DAY], 0.25, until=counts.index.max())) == [counts.index[100]]
    assert feed_down_minutes(counts, [DAY], 0.25).empty  # the whole day: the median is 0, nothing is "down"
