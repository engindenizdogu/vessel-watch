from datetime import date

import numpy as np
import pandas as pd

from models.forecast import baselines, score, time_of_day_mean
from pipeline.features import lagged, target_series

TARGET = {"gate": "sea_entrance", "direction": "in", "groups": ["cargo", "tanker"]}


def hourly_table(days: list[str], counts: dict[tuple[str, str], int], outage: dict[str, int] | None = None) -> pd.DataFrame:
    """The crossings_hourly layout for `days`: zeros except `counts` ({(hour, group): n}) at the target gate."""
    hours = pd.DatetimeIndex([h for d in days for h in pd.date_range(d, periods=24, freq="h")])
    grid = pd.MultiIndex.from_product(
        [hours, ["sea_entrance", "narrows"], ["in", "out"], ["cargo", "tanker", "passenger"]],
        names=["hour", "gate", "direction", "group"],
    )
    table = pd.Series(0, index=grid, name="crossings").reset_index()
    for (hour, group), n in counts.items():
        table.loc[(table.hour == hour) & (table.gate == "sea_entrance") & (table.direction == "in") & (table.group == group), "crossings"] = n
    table["outage_minutes"] = table["hour"].map(pd.Series(outage or {}, dtype=int).rename(pd.Timestamp)).fillna(0).astype(int)
    return table


def test_target_sums_the_groups_at_the_gate_and_direction():
    table = hourly_table(["2024-07-01"], {("2024-07-01 05:00", "cargo"): 2, ("2024-07-01 05:00", "tanker"): 1,
                                          ("2024-07-01 05:00", "passenger"): 7})
    y = target_series(table, TARGET, 1, 10)
    assert len(y) == 24 and y["2024-07-01 05:00"] == 3 and y.sum() == 3


def test_hour_with_a_long_outage_is_missing_and_so_is_its_bin():
    table = hourly_table(["2024-07-01"], {("2024-07-01 04:00", "cargo"): 1},
                         outage={"2024-07-01 05:00": 12, "2024-07-01 07:00": 3})
    hourly = target_series(table, TARGET, 1, 10)
    assert np.isnan(hourly["2024-07-01 05:00"]) and hourly["2024-07-01 07:00"] == 0  # 3 minutes: kept
    three = target_series(table, TARGET, 3, 10)
    assert len(three) == 8 and np.isnan(three["2024-07-01 03:00"]) and three["2024-07-01 06:00"] == 0


def test_lags_are_looked_up_by_time_and_never_cross_a_hole():
    # Two blocks months apart: a row shift would take July 1's values for September 1.
    y = pd.Series([1.0, 2.0, 3.0], index=pd.to_datetime(["2024-07-01 00:00", "2024-09-01 00:00", "2024-09-02 00:00"]))
    assert lagged(y, 24).isna().tolist() == [True, True, False]
    assert lagged(y, 24)["2024-09-02"] == 2.0


def test_baselines_are_scored_on_the_same_test_bins():
    index = pd.date_range("2024-08-01", periods=24 * 9, freq="h")  # 9 days: only the last 2 have last week's value
    y = pd.Series(np.arange(len(index)) % 3, index=index, dtype=float)
    table = score(y, baselines(y, date(2024, 8, 8)), date(2024, 8, 8))
    assert table["bins"].tolist() == [48] * 4
    assert table.loc[table.forecast == "same time last week", "MAE"].iloc[0] == 0  # the series repeats every 3 h


def test_time_of_day_mean_uses_training_bins_only():
    index = pd.to_datetime(["2024-08-01 06:00", "2024-08-02 06:00", "2024-08-03 06:00"])
    y = pd.Series([1.0, 3.0, 100.0], index=index)
    train = np.array([True, True, False])
    assert time_of_day_mean(y, train).tolist() == [2.0, 2.0, 2.0]
