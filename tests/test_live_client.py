from ingest.live_client import parse_time, to_record, update_vessel_type

# Trimmed from messages seen in the first live probe (2026-10-23).
CLASS_A = {
    "MessageType": "PositionReport",
    "MetaData": {"MMSI": 367797720, "time_utc": "2026-10-23 02:55:26.236983564 +0000 UTC"},
    "Message": {"PositionReport": {
        "Latitude": 40.68571833333333, "Longitude": -74.07689, "Sog": 0, "Cog": 199.2,
        "TrueHeading": 226, "NavigationalStatus": 0,
    }},
}
CLASS_B = {
    "MessageType": "StandardClassBPositionReport",
    "MetaData": {"MMSI": 367616050, "time_utc": "2026-10-23 02:55:27.469354181 +0000 UTC"},
    "Message": {"StandardClassBPositionReport": {
        "Latitude": 40.71063166666667, "Longitude": -74.04391666666668, "Sog": 0, "Cog": 292,
        "TrueHeading": 292,
    }},
}
SHIP_STATIC = {
    "MessageType": "ShipStaticData",
    "MetaData": {"MMSI": 367797720},
    "Message": {"ShipStaticData": {"Type": 33}},
}


def test_parse_time_truncates_to_microseconds():
    assert parse_time("2026-10-23 02:55:26.236983564 +0000 UTC") == "2026-10-23T02:55:26.236983Z"


def test_parse_time_without_fraction():
    assert parse_time("2026-10-23 02:55:26 +0000 UTC") == "2026-10-23T02:55:26.000000Z"


def test_class_a_record():
    assert to_record(CLASS_A, {}) == {
        "ts": "2026-10-23T02:55:26.236983Z", "mmsi": 367797720, "lat": 40.68571833333333,
        "lon": -74.07689, "sog": 0, "cog": 199.2, "heading": 226, "vessel_type": None, "status": 0,
    }


def test_class_b_record_has_null_status():
    assert to_record(CLASS_B, {})["status"] is None


def test_vessel_type_comes_from_static_data():
    types: dict[int, int] = {}
    update_vessel_type(SHIP_STATIC, types)
    assert to_record(CLASS_A, types)["vessel_type"] == 33


def test_unavailable_type_is_not_cached():
    msg = {**SHIP_STATIC, "Message": {"ShipStaticData": {"Type": 0}}}
    types: dict[int, int] = {}
    update_vessel_type(msg, types)
    assert types == {}


def test_class_b_static_report_uses_part_b():
    msg = {
        "MessageType": "StaticDataReport",
        "MetaData": {"MMSI": 367175640},
        "Message": {"StaticDataReport": {"ReportB": {"ShipType": 37, "Valid": True}}},
    }
    types: dict[int, int] = {}
    update_vessel_type(msg, types)
    assert types == {367175640: 37}


def test_non_position_messages_are_skipped():
    assert to_record(SHIP_STATIC, {}) is None
    assert to_record({"MessageType": "SubscriptionConfirmation", "Message": {}}, {}) is None
