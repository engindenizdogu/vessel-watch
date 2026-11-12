"""Shared local SparkSession setup for the batch and streaming jobs, and small Spark helpers they share."""

from typing import Any

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def get_spark(
    cfg: dict[str, Any], app_name: str, packages: list[str] | None = None, driver_memory: str | None = None
) -> SparkSession:
    """Return a local SparkSession in UTC with optional Maven packages; `driver_memory` overrides the configured one."""
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", driver_memory or cfg["spark"]["driver_memory"])
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", cfg["spark"]["shuffle_partitions"])
        .config("spark.ui.showConsoleProgress", False)  # progress bars garble the row-count logs
    )
    if packages:
        builder = builder.config("spark.jars.packages", ",".join(packages))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def reports_per_minute(reports: DataFrame) -> pd.Series:
    """Reports per minute, indexed by the minute's start (naive UTC); minutes without reports are absent."""
    # Epoch minutes, not timestamps, cross to pandas: no time zone conversion on the way.
    per_minute = reports.groupBy(F.floor(F.unix_timestamp("ts") / 60).alias("minute")).count().toPandas()
    return pd.Series(per_minute["count"].to_numpy(), index=pd.to_datetime(per_minute["minute"] * 60, unit="s")).sort_index()
