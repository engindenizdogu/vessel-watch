from datetime import datetime, timedelta

import pandas as pd

from models.offroute import episodes, flag_offroute, learn_lanes
from pipeline.config import load_config

CFG = load_config()  # cargo and tanker, 2 knots or more, 0.005 deg cells with neighbours, fewer than 1 ship
T0 = datetime(2024, 7, 1, 12)
LANE = (40.6025, -74.0025)  # the middle of one grid cell
CELL = CFG["offroute"]["cell_deg"]
COLUMNS = ["ts", "mmsi", "name", "lat", "lon", "sog", "vessel_type"]


def report(mmsi, lat, lon, sog=10.0, vessel_type=70, minute=0):
    return (T0 + timedelta(minutes=minute), mmsi, f"V{mmsi}", lat, lon, sog, vessel_type)


def flagged_mmsis(spark, history, new):
    lanes = learn_lanes(spark.createDataFrame(history, COLUMNS), CFG)
    return sorted(r.mmsi for r in flag_offroute(spark.createDataFrame(new, COLUMNS), lanes, CFG).collect())


def test_lanes_count_distinct_ships_and_reach_the_neighbouring_cells(spark):
    history = [report(1, *LANE), report(1, *LANE, minute=1), report(2, *LANE), report(3, LANE[0] + 2 * CELL, LANE[1])]
    lanes = learn_lanes(spark.createDataFrame(history, COLUMNS), CFG).toPandas()
    cy, cx = int(LANE[0] // CELL), int(LANE[1] // CELL)
    ships = lanes.set_index(["cy", "cx"])["ships"]
    assert ships[(cy, cx)] == 2 and ships[(cy + 1, cx + 1)] == 2  # ship 1 twice counts once; the neighbour gets it
    assert ships[(cy + 1, cx)] == 2  # next to both the lane cell and ship 3's cell: the larger count
    assert ships[(cy + 2, cx)] == 1  # ship 3's own cell, two cells from the lane
    assert len(lanes) == 5 * 3  # rows cy-1 to cy+3: the 3x3 around each of the two cells


def test_only_ships_under_way_far_from_every_lane_are_off_route(spark):
    history = [report(1, *LANE)]
    far = (LANE[0] + 4 * CELL, LANE[1])
    new = [
        report(10, *far),                           # cargo ship under way, 4 cells from the lane: off-route
        report(11, *far, vessel_type=84),           # tanker: off-route
        report(12, LANE[0] + CELL, LANE[1]),        # in the next cell over: fine
        report(13, *far, sog=0.5),                  # anchored there: not checked
        report(14, *far, vessel_type=37),           # pleasure craft: not checked
        report(15, *far, vessel_type=None),         # unknown type: not checked
    ]
    assert flagged_mmsis(spark, history, new) == [10, 11]


def test_reports_close_in_time_form_one_alert():
    flagged = pd.DataFrame({
        "mmsi": [1, 1, 1, 2], "name": "V", "vessel_type": 70, "lat": 40.6, "lon": -74.0, "lane_ships": 0,
        "ts": [T0, T0 + timedelta(minutes=10), T0 + timedelta(minutes=60), T0],
    })
    alerts = episodes(flagged, gap_minutes=30)
    assert alerts[["mmsi", "reports"]].values.tolist() == [[1, 2], [1, 1], [2, 1]]
    assert alerts.iloc[0]["end"] == T0 + timedelta(minutes=10)
