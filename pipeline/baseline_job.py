"""Baseline report for the arrival forecast: score the baselines on the test block for the target's vessel
groups and wider ones, in each bin size (`forecast` in config.yaml), from data/crossings_hourly.parquet."""

import pandas as pd

from models.forecast import baselines, is_test, score
from pipeline.config import load_config
from pipeline.features import target_series


def main() -> None:
    cfg = load_config()
    f = cfg["forecast"]
    hourly = pd.read_parquet(cfg["paths"]["crossings_hourly"])
    t = cfg["gates"]["target"]
    group_sets = {"+".join(t["groups"]): t["groups"], **f["wider_groups"]}
    test_start = cfg["split"]["test_start"]
    print(f"target: {t['gate']}, {t['direction']}; test from {test_start}; an hour with "
          f"{f['max_outage_minutes']}+ outage minutes is missing\n")
    for bin_hours in f["bin_hours"]:
        for name, groups in group_sets.items():
            y = target_series(hourly, {**t, "groups": groups}, bin_hours, f["max_outage_minutes"])
            test = is_test(y.index, test_start)
            print(f"{bin_hours} h bins, {name}: mean {y.mean():.2f} per bin, {(y == 0).mean():.0%} zero; "
                  f"{(~test).sum()} train bins, {test.sum()} test bins, {y.isna().sum()} missing")
            print(score(y, baselines(y, test_start), test_start).round(3).to_string(index=False) + "\n")


if __name__ == "__main__":
    main()
