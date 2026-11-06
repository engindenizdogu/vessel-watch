from datetime import date, timedelta

import pytest

from ingest.noaa_history import sample_blocks
from pipeline.config import load_config

RANGES = [
    {"start": date(2023, 1, 1), "end": date(2023, 12, 31)},
    {"start": date(2024, 7, 1), "end": date(2024, 12, 31)},
]


def test_blocks_are_consecutive_days_inside_one_range():
    for block in sample_blocks(RANGES, blocks=8, block_days=14, seed=1):
        assert len(block) == 14
        assert all(b - a == timedelta(days=1) for a, b in zip(block, block[1:]))
        assert any(r["start"] <= block[0] and block[-1] <= r["end"] for r in RANGES)


def test_blocks_do_not_overlap():
    days = [d for b in sample_blocks(RANGES, blocks=20, block_days=14, seed=1) for d in b]
    assert len(days) == len(set(days))


def test_more_blocks_keep_the_earlier_ones():
    few = sample_blocks(RANGES, blocks=2, block_days=14, seed=627)
    more = sample_blocks(RANGES, blocks=4, block_days=14, seed=627)
    assert all(b in more for b in few)


def test_block_can_fill_a_range_exactly():
    r = [{"start": date(2023, 1, 1), "end": date(2023, 1, 14)}]
    assert sample_blocks(r, blocks=1, block_days=14, seed=0) == [
        [date(2023, 1, 1) + timedelta(days=i) for i in range(14)]
    ]


def test_raises_when_blocks_do_not_fit():
    r = [{"start": date(2023, 1, 1), "end": date(2023, 1, 20)}]
    with pytest.raises(ValueError):
        sample_blocks(r, blocks=2, block_days=14, seed=0)


def test_config_ranges_parse_as_dates():
    for r in load_config()["history"]["ranges"]:
        assert isinstance(r["start"], date) and r["start"] <= r["end"]
