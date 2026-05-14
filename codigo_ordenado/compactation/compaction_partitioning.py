from __future__ import annotations

import argparse
import os
import re
from typing import Dict, List, Tuple, Optional

from pyspark.sql import DataFrame

from codigo_ordenado.utils.connection_to_db import spark_on
from codigo_ordenado.utils.json_config import load_json
from codigo_ordenado.utils.hdfs_utils import (
    hdfs_mkdir_p,
    hdfs_ls,
    hdfs_mv,
    hdfs_cp,
    hdfs_rm,
    list_hdfs_files_with_size,
)

# ============================================================
# CONSTANTS
# ============================================================

SAFETY_FACTOR = 0.99
MAX_FILES_PER_READ = 500

FNAME_RE = re.compile(
    r"^(?P<year>\d{4})_(?P<month>\d{2})_.*?_FORST2-"
    r"PN(?P<pn>\d+)-OF(?P<of>\d+)-OP(?P<op>\d+)-SL(?P<sl>\d+)\.parquet$"
)


# ============================================================
# HELPERS
# ============================================================

def build_table_name(partitioning: str, block_mb: int, engine: str, suffix: str) -> str:
    """
    Build output folder name like:
    compacted_partitioned_YYYY_MM_PN_OP_64MB_hive
    compacted_partitioned_YYYY_MM_PN_OP_64MB_hive_forst2hf
    """
    if partitioning == "YYYY_MM":
        part = "YYYY_MM"
    elif partitioning == "YYYY_MM_PN":
        part = "YYYY_MM_PN"
    else:
        part = "YYYY_MM_PN_OP"

    base = "compacted_partitioned_{0}_{1}MB_{2}".format(part, int(block_mb), engine)
    if suffix:
        return "{0}_{1}".format(base, suffix)
    return base


def is_forst2hf_source(src_dir: str) -> bool:
    """
    If you run the compaction on /apps/hive/test/data/parquet/FORST2HF,
    we tag outputs with suffix 'forst2hf'.
    """
    return "FORST2HF" in src_dir


def parse_meta_from_path(path: str) -> Optional[Tuple[int, int, int, int]]:
    base = os.path.basename(path)
    m = FNAME_RE.match(base)
    if not m:
        return None
    year = int(m.group("year"))
    month = int(m.group("month"))
    pn = int(m.group("pn"))
    op = int(m.group("op"))
    return year, month, pn, op


def is_before_start(year: int, month: int, start_year: int, start_month: int) -> bool:
    return (year, month) < (start_year, start_month)


def partition_cols_for(partitioning: str) -> List[str]:
    if partitioning == "YYYY_MM":
        return ["year", "month"]
    if partitioning == "YYYY_MM_PN":
        return ["year", "month", "partnumber"]
    return ["year", "month", "partnumber", "operation"]


def partition_key_for(partitioning: str, year: int, month: int, pn: int, op: int) -> Tuple[int, int, int, int]:
    if partitioning == "YYYY_MM":
        return (year, month, -1, -1)
    if partitioning == "YYYY_MM_PN":
        return (year, month, pn, -1)
    return (year, month, pn, op)


def partition_dir(root: str, partitioning: str, year: int, month: int, pn: int, op: int) -> str:
    if partitioning == "YYYY_MM":
        return "{0}/year={1}/month={2:02d}".format(root, year, month)
    if partitioning == "YYYY_MM_PN":
        return "{0}/year={1}/month={2:02d}/partnumber={3}".format(root, year, month, pn)
    return "{0}/year={1}/month={2:02d}/partnumber={3}/operation={4}".format(root, year, month, pn, op)


def pack_files(files_with_size: List[Tuple[str, int]], target_bytes: int) -> List[List[str]]:
    blocks: List[List[str]] = []
    cur: List[str] = []
    cur_sz = 0

    for path, sz in files_with_size:
        if not cur:
            cur = [path]
            cur_sz = sz
        else:
            if cur_sz + sz <= target_bytes:
                cur.append(path)
                cur_sz += sz
            else:
                blocks.append(cur)
                cur = [path]
                cur_sz = sz

        if cur_sz >= target_bytes:
            blocks.append(cur)
            cur = []
            cur_sz = 0

    if cur:
        blocks.append(cur)

    return blocks


def existing_compact_indices(hdfs_partition_dir: str) -> List[int]:
    entries = hdfs_ls(hdfs_partition_dir)
    idxs: List[int] = []
    for p in entries:
        b = os.path.basename(p)
        mm = re.match(r"^compact_(\d{5})\.parquet$", b)
        if mm:
            idxs.append(int(mm.group(1)))
    idxs.sort()
    return idxs


def max_compact_index(hdfs_partition_dir: str) -> int:
    idxs = existing_compact_indices(hdfs_partition_dir)
    return idxs[-1] if idxs else -1


def read_many_parquets(spark, files: List[str]) -> DataFrame:
    dfs: List[DataFrame] = []
    for i in range(0, len(files), MAX_FILES_PER_READ):
        chunk = files[i:i + MAX_FILES_PER_READ]
        dfs.append(spark.read.parquet(*chunk))

    df = dfs[0]
    for d in dfs[1:]:
        df = df.unionByName(d)
    return df


def drop_partition_cols(df: DataFrame, partition_cols: List[str]) -> DataFrame:
    cols = [c for c in partition_cols if c in df.columns]
    if cols:
        return df.drop(*cols)
    return df


def write_block_and_copy(
    spark,
    input_files: List[str],
    hive_part_dir: str,
    impala_part_dir: str,
    block_idx: int,
    partition_cols: List[str],
    overwrite: bool = False,
) -> None:
    df = read_many_parquets(spark, input_files)
    df = drop_partition_cols(df, partition_cols)

    tmp_dir = "{0}/__tmp_block_{1:05d}".format(hive_part_dir, block_idx)
    final_hive = "{0}/compact_{1:05d}.parquet".format(hive_part_dir, block_idx)
    final_impala = "{0}/compact_{1:05d}.parquet".format(impala_part_dir, block_idx)

    (df.coalesce(1)
       .write
       .mode("overwrite")
       .option("compression", "snappy")
       .parquet(tmp_dir))

    tmp_entries = hdfs_ls(tmp_dir)
    part_file = None
    for p in tmp_entries:
        b = os.path.basename(p)
        if b.startswith("part-") and b.endswith(".parquet"):
            part_file = p
            break

    if overwrite:
        hdfs_rm(final_hive, recursive=False)
        hdfs_rm(final_impala, recursive=False)

    hdfs_mv(part_file, final_hive)
    hdfs_rm(tmp_dir, recursive=True)

    hdfs_mkdir_p(impala_part_dir)
    hdfs_cp(final_hive, final_impala)

    tag = "OVERWRITE" if overwrite else "WRITE"
    print("[{0}] {1}  ->  {2}".format(tag, final_hive, final_impala))


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="codigo_ordenado/config/compaction_forst2hf_2024.json")
    args = parser.parse_args()

    cfg = load_json(args.config)

    src_dir = cfg["src_dir"]
    hive_out_root = cfg["hive_out_root"].rstrip("/")
    impala_out_root = cfg["impala_out_root"].rstrip("/")
    start_year = int(cfg["start_year"])
    start_month = int(cfg["start_month"])
    jobs = cfg["jobs"]

    suffix = "forst2hf" if is_forst2hf_source(src_dir) else ""

    spark = spark_on()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

    print("[INFO] SRC_DIR = {0}".format(src_dir))
    print("[INFO] HIVE_OUT_ROOT = {0}".format(hive_out_root))
    print("[INFO] IMPALA_OUT_ROOT = {0}".format(impala_out_root))
    print("[INFO] START from: year={0}, month={1:02d}".format(start_year, start_month))
    print("[INFO] SUFFIX = {0}".format(suffix if suffix else "(none)"))

    raw_files_with_size = list_hdfs_files_with_size(src_dir)
    print("[INFO] Raw parquet files found (total): {0}".format(len(raw_files_with_size)))

    for job in jobs:
        partitioning = job["partitioning"]
        block_mb = int(job["block_mb"])

        target_bytes = int(block_mb * 1024 * 1024 * SAFETY_FACTOR)
        part_cols = partition_cols_for(partitioning)

        hive_table_name = build_table_name(partitioning, block_mb, "hive", suffix)
        impala_table_name = build_table_name(partitioning, block_mb, "impala", suffix)

        hive_table = "{0}/{1}".format(hive_out_root, hive_table_name)
        impala_table = "{0}/{1}".format(impala_out_root, impala_table_name)

        print("[INFO] JOB: partitioning={0} block_mb={1} target_bytes={2}".format(partitioning, block_mb, target_bytes))
        print("[INFO] HIVE_TABLE_DIR   = {0}".format(hive_table))
        print("[INFO] IMPALA_TABLE_DIR = {0}".format(impala_table))

        hdfs_mkdir_p(hive_table)
        hdfs_mkdir_p(impala_table)

        parts: Dict[Tuple[int, int, int, int], List[Tuple[str, int, int, int, int, int]]] = {}
        skipped_name = 0
        skipped_before_start = 0

        for path, size in raw_files_with_size:
            meta = parse_meta_from_path(path)
            if meta is None:
                skipped_name += 1
                continue

            year, month, pn, op = meta
            if is_before_start(year, month, start_year, start_month):
                skipped_before_start += 1
                continue

            k = partition_key_for(partitioning, year, month, pn, op)
            parts.setdefault(k, []).append((path, size, year, month, pn, op))

        print(
            "[INFO] Partitions detected: {0} | skipped_name_mismatch={1} | skipped_before_start={2}".format(
                len(parts), skipped_name, skipped_before_start
            )
        )

        total_written = 0
        total_skipped = 0
        total_overwritten = 0

        for k in sorted(parts.keys()):
            items = parts[k]
            items.sort(key=lambda x: x[0])

            year = items[0][2]
            month = items[0][3]
            pn = items[0][4]
            op = items[0][5]

            files_with_size = [(p, sz) for (p, sz, _, _, _, _) in items]

            hive_part_dir = partition_dir(hive_table, partitioning, year, month, pn, op)
            impala_part_dir = partition_dir(impala_table, partitioning, year, month, pn, op)

            hdfs_mkdir_p(hive_part_dir)
            hdfs_mkdir_p(impala_part_dir)

            blocks = pack_files(files_with_size, target_bytes)
            last_idx = max_compact_index(hive_part_dir)

            print(
                "[INFO] {0} -> {1} files -> {2} blocks | existing_last_idx={3}".format(
                    k, len(files_with_size), len(blocks), last_idx
                )
            )

            for block_idx, block_files in enumerate(blocks):
                if block_idx < last_idx:
                    total_skipped += 1
                    continue

                if block_idx == last_idx and last_idx >= 0:
                    write_block_and_copy(
                        spark=spark,
                        input_files=block_files,
                        hive_part_dir=hive_part_dir,
                        impala_part_dir=impala_part_dir,
                        block_idx=block_idx,
                        partition_cols=part_cols,
                        overwrite=True,
                    )
                    total_overwritten += 1
                    total_written += 1
                else:
                    write_block_and_copy(
                        spark=spark,
                        input_files=block_files,
                        hive_part_dir=hive_part_dir,
                        impala_part_dir=impala_part_dir,
                        block_idx=block_idx,
                        partition_cols=part_cols,
                        overwrite=False,
                    )
                    total_written += 1

        print(
            "[DONE][{0}][{1}MB] Total compact ops: {2} | skipped_existing={3} | overwritten_open={4}".format(
                partitioning, block_mb, total_written, total_skipped, total_overwritten
            )
        )

    spark.stop()


if __name__ == "__main__":
    main()
