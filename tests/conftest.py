from __future__ import annotations

import os
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    jar_dir = Path(__file__).resolve().parents[1] / ".cache/ivy/jars"
    jars = ",".join(str(path.resolve()) for path in sorted(jar_dir.glob("*.jar")))
    with tempfile.TemporaryDirectory(prefix="amazon-tests-") as temp:
        session = (
            SparkSession.builder.master("local[2]")
            .appName("amazon-recommender-tests")
            .config("spark.jars", jars)
            .config("spark.ui.enabled", "false")
            .config("spark.driver.memory", "8g")
            .config("spark.driver.maxResultSize", "1g")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.driver.host", "127.0.0.1")
            .getOrCreate()
        )
        session.sparkContext.setLogLevel("ERROR")
        session.sparkContext.setCheckpointDir(os.path.join(temp, "checkpoints"))
        yield session
        session.stop()
