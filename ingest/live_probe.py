"""Probe the AISStream live feed over the harbor box: print one example per message type,
a summary, and save every raw message as JSON lines under data/live_sample/."""

import argparse
import asyncio
import json
import os
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import websockets
from dotenv import load_dotenv

from pipeline.config import ROOT, load_config


async def probe(seconds: int, out_path: Path) -> None:
    """Listen for `seconds`, print examples and a summary, and write raw messages to `out_path`."""
    cfg = load_config()
    key = os.environ.get(cfg["live"]["api_key_env"])
    if not key:
        raise SystemExit(f"{cfg['live']['api_key_env']} is not set; add it to .env")

    box = cfg["harbor_bbox"]
    subscribe = {
        "APIKey": key,
        "BoundingBoxes": [[[box["lat_min"], box["lon_min"]], [box["lat_max"], box["lon_max"]]]],
    }

    types: Counter[str] = Counter()
    vessels: set[int] = set()
    start = time.monotonic()
    deadline = start + seconds

    async with websockets.connect(cfg["live"]["url"], compression="deflate") as ws:
        await ws.send(json.dumps(subscribe))
        print(f"Subscribed to {box}; listening for {seconds}s...\n")
        with open(out_path, "w") as out:
            while (remaining := deadline - time.monotonic()) > 0:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                except asyncio.TimeoutError:
                    break
                msg = json.loads(raw)
                if "error" in msg:
                    raise SystemExit(f"AISStream error: {msg['error']}")

                out.write(json.dumps(msg) + "\n")
                msg_type = msg.get("MessageType", "unknown")
                if msg_type not in types:
                    print(f"--- First {msg_type} ---")
                    print(json.dumps(msg, indent=2), "\n")
                types[msg_type] += 1
                mmsi = msg.get("MetaData", {}).get("MMSI")
                if mmsi is not None:
                    vessels.add(mmsi)

    elapsed_min = (time.monotonic() - start) / 60
    total = sum(types.values())
    print("=== Summary ===")
    print(f"Messages: {total} in {elapsed_min:.1f} min ({total / max(elapsed_min, 1e-9):.0f}/min)")
    print(f"Distinct vessels (MMSI): {len(vessels)}")
    for msg_type, n in types.most_common():
        print(f"  {msg_type}: {n}")
    print(f"Raw messages saved to {out_path.relative_to(ROOT)}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=int, default=120, help="how long to listen")
    args = parser.parse_args()

    load_dotenv(ROOT / ".env")
    out_dir = load_config()["paths"]["live_sample"]
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    asyncio.run(probe(args.seconds, out_dir / f"aisstream_{stamp}.jsonl"))


if __name__ == "__main__":
    main()
