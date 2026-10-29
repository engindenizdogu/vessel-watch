"""Download NOAA MarineCadastre daily AIS CSVs into data/raw (about 360 MB zipped, 1 GB unzipped per day).

Days already in data/raw are skipped. Each zip is written to a temporary file, its CSV is unpacked
to AIS_YYYY_MM_DD.csv, and the zip is deleted.
"""

import argparse
import shutil
import time
import zipfile
from datetime import date, timedelta
from pathlib import Path

import requests

from pipeline.config import load_config


def file_url(pattern: str, day: date) -> str:
    """Fill the configured URL pattern for one day."""
    return pattern.format(year=day.year, month=day.month, day=day.day)


def download_day(url: str, csv_path: Path) -> None:
    """Download one daily zip and unpack its single CSV to `csv_path`."""
    zip_path = csv_path.with_suffix(".zip.part")
    with requests.get(url, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        with open(zip_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                f.write(chunk)

    tmp_csv = csv_path.with_suffix(".csv.part")
    with zipfile.ZipFile(zip_path) as zf:
        members = [m for m in zf.namelist() if m.endswith(".csv")]
        if len(members) != 1:
            raise RuntimeError(f"expected one CSV in {url}, found {members}")
        # Copy the member to our own file name instead of extracting, so archive paths are never trusted.
        with zf.open(members[0]) as src, open(tmp_csv, "wb") as dst:
            shutil.copyfileobj(src, dst)
    tmp_csv.rename(csv_path)
    zip_path.unlink()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=date.fromisoformat, required=True, help="first day, YYYY-MM-DD")
    parser.add_argument("--end", type=date.fromisoformat, required=True, help="last day (inclusive), YYYY-MM-DD")
    args = parser.parse_args()

    cfg = load_config()
    raw_dir = cfg["paths"]["raw"]
    raw_dir.mkdir(parents=True, exist_ok=True)

    day = args.start
    while day <= args.end:
        csv_path = raw_dir / f"AIS_{day:%Y_%m_%d}.csv"
        if csv_path.exists():
            print(f"{day}: already downloaded")
        else:
            url = file_url(cfg["noaa"]["file_url"], day)
            start = time.monotonic()
            download_day(url, csv_path)
            size_mb = csv_path.stat().st_size / 1e6
            print(f"{day}: {size_mb:,.0f} MB in {time.monotonic() - start:.0f} s")
        day += timedelta(days=1)


if __name__ == "__main__":
    main()
