from datetime import datetime, timedelta

from pipeline.clean import (
    crop_to_bbox,
    drop_duplicates,
    drop_invalid_mmsi,
    drop_missing_keys,
    drop_position_spikes,
    null_unavailable,
)

SCHEMA = "ts timestamp, mmsi long, lat double, lon double, sog double, cog double, heading int"
BOX = {"lat_min": 40.40, "lat_max": 40.95, "lon_min": -74.30, "lon_max": -73.70}
T0 = datetime(2024, 7, 1, 12, 0)


def row(minute=0, mmsi=367000001, lat=40.70, lon=-74.00, sog=5.0, cog=90.0, heading=90):
    return (T0 + timedelta(minutes=minute), mmsi, lat, lon, sog, cog, heading)


def test_crop_to_bbox(spark):
    df = spark.createDataFrame([row(), row(lat=41.2), row(lon=-71.0), row(lat=91.0, lon=181.0)], SCHEMA)
    assert crop_to_bbox(df, BOX).count() == 1


def test_drop_missing_keys(spark):
    df = spark.createDataFrame([row(), (None, 367000001, 40.7, -74.0, 5.0, 90.0, 90), row(mmsi=None)], SCHEMA)
    assert drop_missing_keys(df).count() == 1


def test_drop_invalid_mmsi(spark):
    mmsis = [367000001, 873000001, 3669999, 1072339762, 970000001, 2000000]
    df = spark.createDataFrame([row(mmsi=m) for m in mmsis], SCHEMA)
    kept = drop_invalid_mmsi(df, 200000000, 799999999).select("mmsi").collect()
    assert [r.mmsi for r in kept] == [367000001]


def test_null_unavailable(spark):
    df = spark.createDataFrame([row(sog=102.3, cog=360.0, heading=511), row(sog=None, cog=None, heading=None)], SCHEMA)
    for r in null_unavailable(df).collect():
        assert (r.sog, r.cog, r.heading) == (None, None, None)


def test_null_unavailable_keeps_valid_values(spark):
    df = spark.createDataFrame([row(sog=0.0, cog=0.0, heading=0), row(sog=102.2, cog=359.9, heading=359)], SCHEMA)
    out = [(r.sog, r.cog, r.heading) for r in null_unavailable(df).collect()]
    assert out == [(0.0, 0.0, 0), (102.2, 359.9, 359)]


def test_drop_duplicates(spark):
    df = spark.createDataFrame([row(), row(), row(minute=1)], SCHEMA)
    assert drop_duplicates(df).count() == 2


def test_isolated_spike_is_dropped_but_neighbours_kept(spark):
    # 0.01 deg lat per minute is about 36 knots; the spike jumps ~0.5 deg (30 nm) for one report.
    lats = [40.50, 40.51, 41.00, 40.53, 40.54]
    df = spark.createDataFrame([row(minute=i, lat=lat) for i, lat in enumerate(lats)], SCHEMA)
    kept = [r.lat for r in drop_position_spikes(df, 50).orderBy("ts").collect()]
    assert kept == [40.50, 40.51, 40.53, 40.54]


def test_first_and_last_reports_are_never_spikes(spark):
    df = spark.createDataFrame([row(minute=0, lat=41.0), row(minute=1, lat=40.5), row(minute=2, lat=41.0)], SCHEMA)
    # The middle report is a spike relative to both neighbours; the ends have one neighbour each.
    assert [r.lat for r in drop_position_spikes(df, 50).orderBy("ts").collect()] == [41.0, 41.0]


def test_spike_check_is_per_vessel(spark):
    df = spark.createDataFrame([row(minute=0, mmsi=367000001, lat=40.5), row(minute=0, mmsi=367000002, lat=40.9),
                                row(minute=1, mmsi=367000001, lat=40.5)], SCHEMA)
    assert drop_position_spikes(df, 50).count() == 3


def test_same_timestamp_is_not_a_spike(spark):
    df = spark.createDataFrame([row(minute=0, lat=40.5), row(minute=1, lat=40.9), row(minute=1, lat=40.5)], SCHEMA)
    assert drop_position_spikes(df, 50).count() == 3
