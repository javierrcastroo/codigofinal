from __future__ import annotations

import argparse
import os
import re

from pyspark.sql import functions as F
from pyspark.sql.types import LongType,DoubleType

from codigo_ordenado.utils.connection_to_db import spark_on
from codigo_ordenado.utils.json_config import load_json
from codigo_ordenado.utils.hdfs_utils import (
    list_hdfs_files_with_size,
    hdfs_exists,
    hdfs_mkdir_p,
    hdfs_rm_if_exists,
    hdfs_finalize_single_file_from_spark_dir,
)

CSV_SEP = ";"
CSV_HEADER = False

# 2024_01_02_06_44_07_FORST2-PN14125165-OF222426298-OP1-SL1
NAME_RE = re.compile(r".*-PN(\d+)-OF(\d+)-OP(\d+)-SL(\d+)$")


def parse_name(base: str):
    m = NAME_RE.match(base)
    pn, of_, op, sl = m.groups()
    return int(of_), int(pn), int(op), int(sl)


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
    tmp_dir = cfg["tmp_dir"]

    spark = spark_on()
    spark.sparkContext.setLogLevel("ERROR")

    hdfs_mkdir_p(dst_dir)
    hdfs_mkdir_p(tmp_dir)

    files = list_hdfs_files_with_size(src_dir)
    src_files = [p for (p, _) in files]

    for src in src_files:
        base = os.path.basename(src)
        dst_path = f"{dst_dir.rstrip('/')}/{base}.parquet"

        if hdfs_exists(dst_path):
            continue

        tmp_out = f"{tmp_dir.rstrip('/')}/{base}"
        hdfs_rm_if_exists(tmp_out, recursive=True)

        ordernumber, partnumber, operation, slotnumber = parse_name(base)

        df = (
            spark.read.format("csv")
            .option("sep", CSV_SEP)
            .option("header", str(CSV_HEADER).lower())
            .load(src)
        ).toDF("time", "f1", "f2", "f3", "f4")

        df = (
            df.withColumn("eventtimestamp", F.col("time").cast(DoubleType()))
              .withColumn("f1", F.col("f1").cast(DoubleType()))
              .withColumn("f2", F.col("f2").cast(DoubleType()))
              .withColumn("f3", F.col("f3").cast(DoubleType()))
              .withColumn("f4", F.col("f4").cast(DoubleType()))
              .drop("time")
              .withColumn("ordernumber", F.lit(ordernumber).cast(LongType()))
              .withColumn("partnumber", F.lit(partnumber).cast(LongType()))
              .withColumn("operation", F.lit(operation).cast(LongType()))
              .withColumn("slotnumber", F.lit(slotnumber).cast(LongType()))
        )

        df = df.select(
            "eventtimestamp",
            "f1",
            "f2",
            "f3",
            "f4",
            "ordernumber",
            "partnumber",
            "operation",
            "slotnumber",
        )

        (
            df.coalesce(1)
            .write.mode("overwrite")
            .option("compression", "snappy")
            .parquet(tmp_out)
        )

        hdfs_finalize_single_file_from_spark_dir(
            spark_out_dir=tmp_out,
            final_file_path=dst_path,
        )

    hdfs_rm_if_exists(tmp_dir, recursive=True)


if __name__ == "__main__":
    main()
