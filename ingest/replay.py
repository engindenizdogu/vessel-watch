"""Replay producer: publish cleaned NOAA days from data/clean to the raw topic in event-time order, at an
accelerated speed, with the live producer's message schema and key (MMSI).

Timestamps are kept as recorded, so replayed reports are years older than live ones; the streaming job keeps
the two apart by event time. Start `make stream RUN=replay` first: a replay run reads only messages
produced after it starts. The range must be consecutive clean days: pacing follows event time, so a range
spanning a hole between sampled blocks would wait out the hole.
"""

import argparse
import json
import time
from collections.abc import Iterator
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
from confluent_kafka import Producer

from ingest.live_client import on_delivery
from pipeline.compact_job import partition_days
from pipeline.config import load_config
from pipeline.stream_job import RAW_SCHEMA

STATUS_EVERY_S = 60


def missing_days(available: list[date], start: date, end: date) -> list[date]:
    """Days from `start` to `end` (inclusive) that are not in `available`."""
    have = set(available)
    return [d for d in (start + timedelta(days=n) for n in range((end - start).days + 1)) if d not in have]


def blocks(days: list[date]) -> list[tuple[date, date]]:
    """Runs of consecutive days in `days` as (first, last), oldest first."""
    runs: list[tuple[date, date]] = []
    for d in sorted(days):
        if runs and d == runs[-1][1] + timedelta(days=1):
            runs[-1] = (runs[-1][0], d)
        else:
            runs.append((d, d))
    return runs


def count_reports(clean: Path, start: date, end: date) -> int:
    """Number of clean reports from `start` to `end` (dates inclusive)."""
    with duckdb.connect() as con:
        return con.sql(
            f"select count(*) from read_parquet('{clean}/date=*/*.parquet', hive_partitioning = true) "
            "where date between ? and ?",
            params=[start, end],
        ).fetchall()[0][0]


def read_reports(clean: Path, start: date, end: date, batch_rows: int = 100_000) -> Iterator[dict[str, Any]]:
    """Clean reports from `start` to `end` (dates inclusive) in the shared schema, oldest first."""
    columns = ", ".join(RAW_SCHEMA.fieldNames())
    with duckdb.connect() as con:
        reader = con.sql(
            f"select {columns} from read_parquet('{clean}/date=*/*.parquet', hive_partitioning = true) "
            "where date between ? and ? order by ts, mmsi",
            params=[start, end],
        ).to_arrow_reader(batch_rows)
        for batch in reader:
            yield from batch.to_pylist()


def to_message(report: dict[str, Any]) -> dict[str, Any]:
    """A clean report as a raw-topic message: the timestamp (naive UTC) as ISO 8601 like the live producer's."""
    return {**report, "ts": f"{report['ts']:%Y-%m-%dT%H:%M:%S.%fZ}"}


def wait_seconds(ts: datetime, first_ts: datetime, elapsed_s: float, speed: float) -> float:
    """Seconds to wait before sending a report at `ts`, `elapsed_s` into a replay that started at `first_ts`."""
    return (ts - first_ts).total_seconds() / speed - elapsed_s


def send(producer: Producer, topic: str, message: dict[str, Any]) -> None:
    """Produce one message keyed by MMSI, waiting for deliveries while the local queue is full."""
    while True:
        try:
            producer.produce(topic, key=str(message["mmsi"]), value=json.dumps(message), on_delivery=on_delivery)
            return
        except BufferError:
            producer.poll(1)


def replay(reports: Iterator[dict[str, Any]], producer: Producer, topic: str, speed: float, total: int) -> int:
    """Send `reports` in order, paced so event time runs `speed` times faster than wall time (0: no pacing).

    Returns the number of reports sent.
    """
    sent = 0
    first_ts = None
    t0 = last_status = time.monotonic()
    for report in reports:
        if first_ts is None:
            first_ts = report["ts"]
        if speed > 0:
            wait = wait_seconds(report["ts"], first_ts, time.monotonic() - t0, speed)
            if wait > 0:
                time.sleep(wait)
        send(producer, topic, to_message(report))
        producer.poll(0)
        sent += 1
        if time.monotonic() - last_status >= STATUS_EVERY_S:
            print(f"Sent {sent:,} of {total:,} reports; replay clock {report['ts']:%Y-%m-%d %H:%M} UTC")
            last_status = time.monotonic()
    return sent


def main() -> None:
    cfg = load_config()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=date.fromisoformat, required=True, help="first day (YYYY-MM-DD)")
    parser.add_argument("--end", type=date.fromisoformat, help="last day (YYYY-MM-DD); default: --start")
    parser.add_argument("--speed", type=float, default=cfg["replay"]["speed"],
                        help="event time runs this many times faster than wall time; 0 = as fast as possible")
    args = parser.parse_args()
    end = args.end or args.start

    clean, topic = cfg["paths"]["clean"], cfg["kafka"]["topic_raw"]
    available = list(partition_days(clean))
    if missing := missing_days(available, args.start, end):
        runs = "\n".join(f"  {a} to {b}" for a, b in blocks(available))
        raise SystemExit(f"{len(missing)} days from {args.start} to {end} are not in {clean} (first: {missing[0]}). "
                         f"Pick a range inside one block of consecutive days:\n{runs}")
    total = count_reports(clean, args.start, end)
    pace = "as fast as possible"
    if args.speed > 0:
        minutes = ((end - args.start).days + 1) * 24 * 60 / args.speed
        pace = f"{args.speed:g}x, about " + (f"{minutes / 60:.1f} h" if minutes >= 60 else f"{minutes:.0f} min")
    print(f"Replaying {total:,} reports from {args.start} to {end} to {topic} ({pace})")

    producer = Producer({"bootstrap.servers": cfg["kafka"]["bootstrap_servers"]})
    try:
        sent = replay(read_reports(clean, args.start, end), producer, topic, args.speed, total)
        print(f"Sent {sent:,} reports")
    except KeyboardInterrupt:
        pass
    finally:
        if remaining := producer.flush(30):
            print(f"{remaining} messages were not delivered")


if __name__ == "__main__":
    main()
