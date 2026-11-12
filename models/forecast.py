"""Arrival forecast evaluation: the time-based split, the baselines, and the error metrics.

Every forecast is scored on the same bins: test bins (at or after the split) where the target and every
forecast have a value. The seasonal-naive forecasts need the bin a week or a day earlier, so the first week of
a block can never be scored for the weekly one.
"""

from datetime import date

import numpy as np
import pandas as pd

from pipeline.features import lagged

WEEK_H, DAY_H = 168, 24


def is_test(index: pd.DatetimeIndex, test_start: date) -> np.ndarray:
    """True for bins on or after `test_start` (train on earlier days, test on later ones; never shuffled)."""
    return index >= pd.Timestamp(test_start)


def time_of_day_mean(y: pd.Series, train: np.ndarray) -> pd.Series:
    """Forecast each bin with the mean of the training bins that start at the same time of day."""
    clock = y.index.time
    means = y[train].groupby(clock[train]).mean()
    return pd.Series(pd.Series(clock).map(means).to_numpy(), index=y.index)


def baselines(y: pd.Series, test_start: date) -> dict[str, pd.Series]:
    """The baselines: same bin last week (seasonal naive), same bin yesterday, the training time-of-day mean,
    and always zero. Zero is the median of a sparse count, so it shows when MAE rewards forecasting nothing."""
    return {
        "same time last week": lagged(y, WEEK_H),
        "same time yesterday": lagged(y, DAY_H),
        "time-of-day mean (train)": time_of_day_mean(y, ~is_test(y.index, test_start)),
        "always zero": pd.Series(0.0, index=y.index),
    }


def score(y: pd.Series, forecasts: dict[str, pd.Series], test_start: date) -> pd.DataFrame:
    """MAE and RMSE of each forecast on the shared test bins (test, target known, every forecast known)."""
    shared = is_test(y.index, test_start) & y.notna().to_numpy()
    for f in forecasts.values():
        shared &= f.notna().to_numpy()
    rows = []
    for name, f in forecasts.items():
        err = (f - y)[shared]
        rows.append({"forecast": name, "MAE": err.abs().mean(), "RMSE": np.sqrt((err**2).mean()), "bins": int(shared.sum())})
    return pd.DataFrame(rows)
