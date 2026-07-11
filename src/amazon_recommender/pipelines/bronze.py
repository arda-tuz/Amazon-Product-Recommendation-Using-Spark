"""Distributed semantic parsing into the Bronze contracts."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator

from pyspark import StorageLevel
from pyspark.rdd import RDD
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from amazon_recommender.ingestion.hadoop import read_hadoop_blocks
from amazon_recommender.ingestion.parser import parse_block
from amazon_recommender.ingestion.schemas import (
    BRONZE_PRODUCT_SCHEMA,
    HEADER_SCHEMA,
    INGESTION_ENVELOPE_SCHEMA,
    QUARANTINE_SCHEMA,
)


_ID_PREFIX = re.compile(r"^Id:\s+(\d+)")
SMOKE_ANCHOR_IDS = frozenset({0, 1, 5})
SELECTION_SCHEMA = StructType(
    [
        StructField("product_id", IntegerType(), False),
        StructField("selection_reason", StringType(), False),
    ]
)


@dataclass(frozen=True)
class BronzeFrames:
    products: DataFrame
    quarantine: DataFrame
    header: DataFrame
    smoke_selection: DataFrame
    cached_rdds: tuple[RDD[Any], ...] = ()

    def release(self) -> None:
        for rdd in self.cached_rdds:
            rdd.unpersist()


def stable_product_hash(product_id: int, seed: int = 42) -> str:
    return hashlib.sha256(f"{product_id}\x1f{seed}".encode("utf-8")).hexdigest()


def smoke_selection_reason(
    raw_block: str, *, seed: int = 42, threshold_exclusive: int = 328
) -> str | None:
    match = _ID_PREFIX.match(raw_block)
    if match is None:
        return "header" if raw_block.startswith("#") else None
    product_id = int(match.group(1))
    if product_id in SMOKE_ANCHOR_IDS:
        return "required_anchor"
    first_16_bits = int(stable_product_hash(product_id, seed)[:4], 16)
    return "stable_hash" if first_16_bits < threshold_exclusive else None


def _parse_partition(
    rows: Iterable[tuple[int, str]], source_path: str
) -> Iterator[tuple[str, dict[str, Any]]]:
    for offset, raw in rows:
        outcome = parse_block(
            raw,
            source_path=source_path,
            source_offset=int(offset),
            record_ordinal=None,
        )
        yield outcome.kind, outcome.row


def _parse_envelope_partition(
    rows: Iterable[tuple[int, str]], source_path: str
) -> Iterator[dict[str, Any]]:
    for kind, row in _parse_partition(rows, source_path):
        yield {
            "kind": kind,
            "product": row if kind == "product" else None,
            "quarantine": row if kind == "quarantine" else None,
            "header": row if kind == "header" else None,
        }


def write_bronze_envelope(
    spark: SparkSession,
    source: Path,
    delimiter: bytes,
    destination: Path,
    *,
    split_max_bytes: int,
) -> None:
    """Parse every source block exactly once and stream an envelope to Parquet."""

    blocks = read_hadoop_blocks(
        spark, source, delimiter, split_max_bytes=split_max_bytes
    )
    rows = blocks.mapPartitions(
        lambda iterator: _parse_envelope_partition(iterator, str(source.resolve()))
    )
    spark.createDataFrame(rows, INGESTION_ENVELOPE_SCHEMA).write.mode("error").option(
        "compression", "snappy"
    ).parquet(str(destination))


def read_bronze_envelope(spark: SparkSession, source: Path) -> BronzeFrames:
    envelope = spark.read.parquet(str(source))
    products = envelope.filter(F.col("kind") == "product").select("product.*")
    quarantine = envelope.filter(F.col("kind") == "quarantine").select(
        "quarantine.*"
    )
    header = envelope.filter(F.col("kind") == "header").select("header.*")
    return BronzeFrames(
        products,
        quarantine,
        header,
        spark.createDataFrame([], SELECTION_SCHEMA),
    )


def load_bronze(
    spark: SparkSession,
    source: Path,
    delimiter: bytes,
    *,
    split_max_bytes: int,
    sample: bool,
    seed: int = 42,
    threshold_exclusive: int = 328,
) -> BronzeFrames:
    blocks: RDD[tuple[int, str]] = read_hadoop_blocks(
        spark, source, delimiter, split_max_bytes=split_max_bytes
    )
    if sample:
        selected = blocks.map(
            lambda pair: (
                pair,
                smoke_selection_reason(
                    pair[1], seed=seed, threshold_exclusive=threshold_exclusive
                ),
            )
        ).filter(lambda pair: pair[1] is not None)
        selected.persist(StorageLevel.MEMORY_AND_DISK)
        chosen_blocks = selected.map(lambda pair: pair[0])
        selection_rows = selected.filter(
            lambda pair: not pair[0][1].startswith("#")
        ).map(
            lambda pair: (
                int(_ID_PREFIX.match(pair[0][1]).group(1)),  # type: ignore[union-attr]
                str(pair[1]),
            )
        )
        cached_rdds: tuple[RDD[Any], ...] = (selected,)
    else:
        chosen_blocks = blocks
        selection_rows = spark.sparkContext.emptyRDD()
        cached_rdds = ()
    parsed = chosen_blocks.mapPartitions(
        lambda iterator: _parse_partition(iterator, str(source.resolve()))
    ).persist(StorageLevel.MEMORY_AND_DISK)
    products = spark.createDataFrame(
        parsed.filter(lambda pair: pair[0] == "product").map(lambda pair: pair[1]),
        BRONZE_PRODUCT_SCHEMA,
    )
    quarantine = spark.createDataFrame(
        parsed.filter(lambda pair: pair[0] == "quarantine").map(lambda pair: pair[1]),
        QUARANTINE_SCHEMA,
    )
    header = spark.createDataFrame(
        parsed.filter(lambda pair: pair[0] == "header").map(lambda pair: pair[1]),
        HEADER_SCHEMA,
    )
    selection = spark.createDataFrame(selection_rows, SELECTION_SCHEMA)
    return BronzeFrames(products, quarantine, header, selection, (*cached_rdds, parsed))
