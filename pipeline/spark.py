"""Shared local SparkSession setup for the batch and streaming jobs."""

from typing import Any

from pyspark.sql import SparkSession


def get_spark(cfg: dict[str, Any], app_name: str, packages: list[str] | None = None) -> SparkSession:
    """Return a local SparkSession in UTC with the configured driver memory and optional Maven packages."""
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", cfg["spark"]["driver_memory"])
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", cfg["spark"]["shuffle_partitions"])
        .config("spark.ui.showConsoleProgress", False)  # progress bars garble the row-count logs
    )
    if packages:
        builder = builder.config("spark.jars.packages", ",".join(packages))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
