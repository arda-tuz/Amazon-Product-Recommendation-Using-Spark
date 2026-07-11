"""Split-aware Hadoop TextInputFormat access with exact source offsets."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark import RDD
    from pyspark.sql import SparkSession


HADOOP_INPUT_FORMAT = "org.apache.hadoop.mapreduce.lib.input.TextInputFormat"
HADOOP_KEY_CLASS = "org.apache.hadoop.io.LongWritable"
HADOOP_VALUE_CLASS = "org.apache.hadoop.io.Text"


def hadoop_conf(delimiter: bytes, split_max_bytes: int = 134_217_728) -> dict[str, str]:
    try:
        delimiter_text = delimiter.decode("ascii")
    except UnicodeDecodeError as error:
        raise ValueError("Hadoop record delimiter must be ASCII") from error
    return {
        "textinputformat.record.delimiter": delimiter_text,
        "mapreduce.input.fileinputformat.split.maxsize": str(split_max_bytes),
        "mapreduce.input.fileinputformat.split.minsize": "1",
    }


def read_hadoop_blocks(
    spark: "SparkSession",
    source: Path,
    delimiter: bytes,
    *,
    split_max_bytes: int = 134_217_728,
) -> "RDD[tuple[int, str]]":
    uri = source.resolve().as_uri()
    return spark.sparkContext.newAPIHadoopFile(
        uri,
        HADOOP_INPUT_FORMAT,
        HADOOP_KEY_CLASS,
        HADOOP_VALUE_CLASS,
        conf=hadoop_conf(delimiter, split_max_bytes),
    ).map(lambda pair: (int(pair[0]), str(pair[1])))
