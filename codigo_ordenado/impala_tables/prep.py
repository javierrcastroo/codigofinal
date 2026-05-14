from __future__ import annotations

import argparse

from pyspark.sql import functions as F
from pyspark.sql.types import LongType, DoubleType, IntegerType

from codigo_ordenado.utils.connection_to_db import spark_on
from codigo_ordenado.utils.json_config import load_json
from codigo_ordenado.utils.hdfs_utils import (
    hdfs_mkdir_p,
    hdfs_rm_if_exists,
    list_hdfs_files_with_size,
)

CSV_SEP = ";"

NAME_RE = r".*-PN(\d+)-OF(\d+)-OP(\d+)-SL(\d+)$"
YEAR_RE = r"([0-9]{4})_([0-9]{2})_"

OUTPUT_PARTITIONS = 100


def main() -> None:

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        default="codigo_ordenado/config/preprocessing_forst2hf_2024.json",
    )
    args = parser.parse_args()

    cfg = load_json(args.config)

    src_dir = cfg["src_dir"]
    dst_dir = cfg["dst_dir"]

    spark = spark_on()
    spark.sparkContext.setLogLevel("ERROR")

    spark.conf.set("spark.sql.shuffle.partitions", str(OUTPUT_PARTITIONS))

    hdfs_rm_if_exists(dst_dir, recursive=True)
    hdfs_mkdir_p(dst_dir)

    # FILTER ONLY NON-EMPTY FILES
    files = list_hdfs_files_with_size(src_dir)

    valid_files = [
        path
        for path, size in files
        if size > 0
    ]

    if not valid_files:
        raise RuntimeError("No valid input files found")

    # READ CSV
    df = (
        spark.read.format("csv")
        .option("sep", CSV_SEP)
        .option("header", "true")
        .load(valid_files)
    )

    # CAST TYPES
    df = (
        df.withColumn("timestamp", F.col("time").cast(DoubleType()))
          .drop("time")
          .withColumn("f1", F.col("f1").cast(DoubleType()))
          .withColumn("f2", F.col("f2").cast(DoubleType()))
          .withColumn("f3", F.col("f3").cast(DoubleType()))
          .withColumn("f4", F.col("f4").cast(DoubleType()))
    )

    # EXTRACT METADATA
    base = F.element_at(F.split(F.input_file_name(), "/"), -1)

    df = (
        df.withColumn("year", F.regexp_extract(base, YEAR_RE, 1).cast(IntegerType()))
          .withColumn("month", F.regexp_extract(base, YEAR_RE, 2).cast(IntegerType()))
          .withColumn("partnumber", F.regexp_extract(base, NAME_RE, 1).cast(LongType()))
          .withColumn("ordernumber", F.regexp_extract(base, NAME_RE, 2).cast(LongType()))
          .withColumn("operation", F.regexp_extract(base, NAME_RE, 3).cast(LongType()))
          .withColumn("slotnumber", F.regexp_extract(base, NAME_RE, 4).cast(LongType()))
    )

    # ORDER
    df = df.select(
        "timestamp",
        "f1",
        "f2",
        "f3",
        "f4",
        "ordernumber",
        "partnumber",
        "operation",
        "slotnumber",
        "year",
        "month",
    )

    # WRITE PARQUET
    (
        df.repartition(OUTPUT_PARTITIONS)
          .write
          .mode("overwrite")
          .option("compression", "snappy")
          .parquet(dst_dir)
    )


if __name__ == "__main__":
    main()
