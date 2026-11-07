from datetime import date, datetime, timedelta

import pandas as pd

from pipeline.config import load_config
from pipeline.gates import CROSSING_COLUMNS, CROSSING_SCHEMA, find_crossings, hourly_counts, vessel_group

T0 = datetime(2024, 7, 1, 12)
# A west-east gate at latitude 40.60, harbor to the north.
GATES = {"max_gap_minutes": 30, "lines": [{"name": "g", "line": [[-74.05, 40.60], [-74.00, 40.60]], "inside": [-74.02, 40.62]}]}


def track(points, mmsi=367000001, vessel_type=70):
    """One vessel's reports: (minutes after T0, lon, lat) tuples."""
    return pd.DataFrame({
        "ts": [T0 + timedelta(minutes=m) for m, _, _ in points], "mmsi": mmsi, "name": "EXAMPLE",
        "vessel_type": vessel_type, "lon": [x for _, x, _ in points], "lat": [y for _, _, y in points],
    })


def test_crossing_toward_the_harbor_is_in_and_back_is_out():
    out = find_crossings(track([(0, -74.02, 40.59), (1, -74.02, 40.61), (60, -74.02, 40.61), (61, -74.02, 40.59)]), GATES)
    assert out["direction"].tolist() == ["in", "out"]
    assert out["ts"].tolist() == [T0 + timedelta(minutes=1), T0 + timedelta(minutes=61)]  # timed at the report after
    assert list(out.columns) == CROSSING_COLUMNS


def test_passing_beyond_the_end_of_the_gate_is_not_a_crossing():
    assert find_crossings(track([(0, -73.99, 40.59), (1, -73.99, 40.61)]), GATES).empty  # east of the gate's end


def test_reports_too_far_apart_are_not_joined():
    assert find_crossings(track([(0, -74.02, 40.59), (45, -74.02, 40.61)]), GATES).empty


def test_input_order_does_not_matter():
    reports = track([(0, -74.02, 40.59), (1, -74.02, 40.61)])
    assert find_crossings(reports.iloc[::-1], GATES).equals(find_crossings(reports, GATES))


def test_vessel_group_maps_ranges_and_leaves_the_rest_other():
    groups = load_config()["vessel_groups"]
    types = pd.Series([70, 85, 52, 37, 90, None], dtype="Int32")
    assert vessel_group(types, groups).tolist() == ["cargo", "tanker", "tug_tow", "pleasure_sail", "other", "other"]


def test_hourly_counts_include_zero_hours():
    crossings = pd.DataFrame({"ts": [datetime(2024, 7, 1, 3, 10)], "gate": ["g"], "direction": ["in"], "vessel_type": [70]})
    hourly = hourly_counts(crossings, [date(2024, 7, 1)], ["g"], {"cargo": [[70, 79]]})
    assert len(hourly) == 24 * 2 * 2 and hourly["crossings"].sum() == 1  # hours x directions x (cargo, other)
    hit = hourly[(hourly.hour == pd.Timestamp(2024, 7, 1, 3)) & (hourly.direction == "in") & (hourly.group == "cargo")]
    assert hit["crossings"].tolist() == [1]


def test_configured_inside_points_are_off_the_lines():
    for gate in load_config()["gates"]["lines"]:
        (ax, ay), (bx, by) = gate["line"]
        px, py = gate["inside"]
        assert abs((bx - ax) * (py - ay) - (by - ay) * (px - ax)) > 1e-6


def test_runs_per_vessel_in_spark(spark):
    reports = pd.concat([track([(0, -74.02, 40.59), (1, -74.02, 40.61)]), track([(0, -73.9, 40.5)], mmsi=367000002)])
    out = spark.createDataFrame(reports).groupBy("mmsi").applyInPandas(
        lambda v: find_crossings(v, GATES), schema=CROSSING_SCHEMA).collect()
    assert [(r.mmsi, r.gate, r.direction) for r in out] == [(367000001, "g", "in")]
