"""Live producer: stream AISStream position reports over the harbor box into Kafka.

Each position report becomes one JSON message on the raw topic, keyed by MMSI, with the shared
schema: ts, mmsi, lat, lon, sog, cog, heading, vessel_type, status. Values are passed through as
received; cleaning happens in Spark.
"""

import asyncio
import json
import os
import time
from typing import Any

import websockets
from confluent_kafka import KafkaError, Message, Producer
from dotenv import load_dotenv

from pipeline.config import ROOT, load_config

POSITION_TYPES = ("PositionReport", "StandardClassBPositionReport")
STATIC_TYPES = ("ShipStaticData", "StaticDataReport")
STATUS_EVERY_S = 60


def parse_time(time_utc: str) -> str:
    """Convert AISStream's '2026-10-23 02:55:26.236983564 +0000 UTC' to ISO 8601 UTC (microseconds)."""
    date, clock = time_utc.split()[:2]
    whole, _, frac = clock.partition(".")
    return f"{date}T{whole}.{frac[:6].ljust(6, '0')}Z"


def update_vessel_type(msg: dict[str, Any], vessel_types: dict[int, int]) -> None:
    """Remember the vessel type from a static data message, keyed by MMSI."""
    msg_type = msg["MessageType"]
    body = msg["Message"][msg_type]
    if msg_type == "ShipStaticData":
        ship_type = body.get("Type")
    else:  # StaticDataReport (Class B): the type is in part B
        report_b = body.get("ReportB", {})
        ship_type = report_b.get("ShipType") if report_b.get("Valid") else None
    if ship_type:  # 0 means "not available"
        vessel_types[msg["MetaData"]["MMSI"]] = ship_type


def to_record(msg: dict[str, Any], vessel_types: dict[int, int]) -> dict[str, Any] | None:
    """Map a position report to the shared schema; return None for other message types."""
    msg_type = msg.get("MessageType")
    if msg_type not in POSITION_TYPES:
        return None
    meta = msg["MetaData"]
    body = msg["Message"][msg_type]
    mmsi = meta["MMSI"]
    return {
        "ts": parse_time(meta["time_utc"]),
        "mmsi": mmsi,
        "lat": body["Latitude"],
        "lon": body["Longitude"],
        "sog": body.get("Sog"),
        "cog": body.get("Cog"),
        "heading": body.get("TrueHeading"),
        "vessel_type": vessel_types.get(mmsi),
        "status": body.get("NavigationalStatus"),  # Class B reports have no status
    }


def on_delivery(err: KafkaError | None, _msg: Message) -> None:
    """Report messages Kafka could not accept."""
    if err is not None:
        print(f"Delivery failed: {err}")


async def stream(cfg: dict[str, Any], key: str, producer: Producer) -> None:
    """Forward position reports to Kafka until interrupted, reconnecting when the socket drops."""
    box = cfg["harbor_bbox"]
    subscribe = {
        "APIKey": key,
        "BoundingBoxes": [[[box["lat_min"], box["lon_min"]], [box["lat_max"], box["lon_max"]]]],
    }
    topic = cfg["kafka"]["topic_raw"]
    vessel_types: dict[int, int] = {}
    vessels: set[int] = set()
    sent = 0
    last_status = time.monotonic()

    async for ws in websockets.connect(cfg["live"]["url"], compression="deflate"):
        try:
            await ws.send(json.dumps(subscribe))
            print(f"Subscribed to {box}; producing to {topic}")
            async for raw in ws:
                msg = json.loads(raw)
                if "error" in msg:
                    raise SystemExit(f"AISStream error: {msg['error']}")
                if msg.get("MessageType") in STATIC_TYPES:
                    update_vessel_type(msg, vessel_types)
                record = to_record(msg, vessel_types)
                if record is not None:
                    producer.produce(
                        topic, key=str(record["mmsi"]), value=json.dumps(record), on_delivery=on_delivery
                    )
                    sent += 1
                    vessels.add(record["mmsi"])
                producer.poll(0)

                if time.monotonic() - last_status >= STATUS_EVERY_S:
                    print(f"Sent {sent} position reports from {len(vessels)} vessels "
                          f"({len(vessel_types)} with known type)")
                    last_status = time.monotonic()
        except websockets.ConnectionClosed:
            print("Connection closed; reconnecting...")


def main() -> None:
    load_dotenv(ROOT / ".env")
    cfg = load_config()
    key = os.environ.get(cfg["live"]["api_key_env"])
    if not key:
        raise SystemExit(f"{cfg['live']['api_key_env']} is not set; add it to .env")

    producer = Producer({"bootstrap.servers": cfg["kafka"]["bootstrap_servers"]})
    try:
        asyncio.run(stream(cfg, key, producer))
    except KeyboardInterrupt:
        pass
    finally:
        if remaining := producer.flush(10):
            print(f"{remaining} messages were not delivered")


if __name__ == "__main__":
    main()
