import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", 1)
        .config("spark.ui.enabled", False)
        .getOrCreate()
    )
    yield session
    session.stop()
