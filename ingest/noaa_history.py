"""Download and clean a random sample of NOAA days (config `history`), one day at a time.

Each day is downloaded to a temporary folder, cleaned into data/clean with the batch cleaning steps, and
deleted, so peak disk use is one raw day (about 1.4 GB). Days already in data/clean are skipped, so the
job can be stopped with Ctrl+C and rerun.
"""

import random
import tempfile
import time
from datetime import date, timedelta
from pathlib import Path

from ingest.noaa_download import download_day, file_url
from pipeline.batch_job import clean_csvs
from pipeline.config import load_config
from pipeline.spark import get_spark


def sample_blocks(ranges: list[dict[str, date]], blocks: int, block_days: int, seed: int) -> list[list[date]]:
    """Draw `blocks` non-overlapping runs of `block_days` consecutive days, each inside one range.

    Draws are sequential with a seeded RNG, so raising `blocks` keeps the earlier blocks. Returned in date order.
    """
    span = timedelta(days=block_days - 1)
    starts = [
        r["start"] + timedelta(days=i)
        for r in ranges
        for i in range((r["end"] - span - r["start"]).days + 1)
    ]
    rng = random.Random(seed)
    chosen: list[date] = []
    for _ in range(blocks):
        free = [s for s in starts if all(abs((s - c).days) >= block_days for c in chosen)]
        if not free:
            raise ValueError(f"no room for {blocks} blocks of {block_days} days in {ranges}")
        chosen.append(rng.choice(free))
    return [[s + timedelta(days=i) for i in range(block_days)] for s in sorted(chosen)]


def main() -> None:
    cfg = load_config()
    h = cfg["history"]
    blocks = sample_blocks(h["ranges"], h["blocks"], h["block_days"], h["seed"])
    for b in blocks:
        print(f"block: {b[0]} to {b[-1]}")

    raw_dir, clean_dir = cfg["paths"]["raw"], cfg["paths"]["clean"]
    todo = [d for b in blocks for d in b if not (clean_dir / f"date={d}").exists()]
    print(f"{len(todo)} of {sum(len(b) for b in blocks)} days to download and clean")
    if not todo:
        return

    raw_dir.mkdir(parents=True, exist_ok=True)
    spark = get_spark(cfg, "vessel-watch-history")
    for i, day in enumerate(todo, 1):
        start = time.monotonic()
        with tempfile.TemporaryDirectory(dir=raw_dir) as tmp:  # removed even if a step fails
            csv_path = Path(tmp) / f"AIS_{day:%Y_%m_%d}.csv"
            download_day(file_url(cfg["noaa"]["file_url"], day), csv_path)
            clean_csvs(spark, cfg, [str(csv_path)])
        print(f"{day} ({i}/{len(todo)}): done in {time.monotonic() - start:.0f} s")


if __name__ == "__main__":
    main()
