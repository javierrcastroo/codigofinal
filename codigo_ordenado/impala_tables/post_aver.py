"""Post-load enrichment and validation for aggregated Impala tables.

This module runs after aggregation table creation to apply updates, checks, and
additional inserts required by downstream benchmark consumers.
"""

from __future__ import annotations

from datetime import datetime
from typing import List, Tuple

from codigo_ordenado.utils.connection_to_db import impala_on, impala_off
from codigo_ordenado.utils.hdfs_utils import (
    hdfs_du_bytes,
    hdfs_ls,
    hdfs_mkdir_p,
    hdfs_mv,
    hdfs_rm_if_exists,
)

DB_NAME = "sandbox_db"

INCOMING_DIR = "/apps/impala/sandbox_db/data_auto/parquet_with_snappy_111_post_sintetico"
BATCH_ROOT = "/apps/impala/sandbox_db/data_auto/batches/auto_sintetico"
PROCESSED_ROOT = "/apps/impala/sandbox_db/data_auto/batches_processed/auto_sintetico"

STAGING_TBL = "auto_staging_ext_sintetico_batch"
TARGET_TBL = "auto_yyyy_mm_pn_op_sintetico"

USE_SHUFFLE_HINT = False
PARQUET_FILE_SIZE = "64m"

NUM_NODES_WRITE = 1
NUM_NODES_STATS = 0
MAX_FRAGMENT_INSTANCES_PER_NODE: int | None = 1

COMPACT_THRESHOLD_BYTES = 64 * 1024 * 1024

DO_COMPUTE_STATS = False
DO_COMPUTE_INCREMENTAL_STATS = True


def batch_id() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def incoming_has_data() -> bool:
    return len(hdfs_ls(INCOMING_DIR)) > 0


def freeze_incoming_to_batch() -> str:
    if not incoming_has_data():
        return ""

    bid = batch_id()
    batch_dir = f"{BATCH_ROOT}/batch={bid}"

    hdfs_mkdir_p(BATCH_ROOT)
    hdfs_mv(INCOMING_DIR, batch_dir)
    hdfs_mkdir_p(INCOMING_DIR)

    return batch_dir


def archive_batch(batch_dir: str) -> str:
    processed_dir = batch_dir.replace(BATCH_ROOT, PROCESSED_ROOT)
    hdfs_mkdir_p(PROCESSED_ROOT)
    hdfs_mv(batch_dir, processed_dir)
    return processed_dir


def get_table_location(cur) -> str:
    cur.execute(f"SHOW CREATE TABLE {DB_NAME}.{TARGET_TBL}")
    create_sql = cur.fetchall()[0][0]
    for line in create_sql.splitlines():
        line = line.strip()
        if line.upper().startswith("LOCATION"):
            return line.split("'", 2)[1]
    raise RuntimeError("Could not extract LOCATION from SHOW CREATE TABLE.")


def partition_hdfs_path(table_location: str, y: int, m: int, pn: int, op: int) -> str:
    return (
        f"{table_location.rstrip('/')}/"
        f"year={y}/month={m}/partnumber={pn}/operation={op}"
    )


def create_staging_for_batch(cur, batch_dir: str) -> None:
    cur.execute(f"DROP TABLE IF EXISTS {DB_NAME}.{STAGING_TBL}")
    cur.execute(
        f"""
        CREATE EXTERNAL TABLE {DB_NAME}.{STAGING_TBL} (
            `timestamp` DOUBLE,
            f1 DOUBLE,
            f2 DOUBLE,
            f3 DOUBLE,
            f4 DOUBLE,
            ordernumber BIGINT,
            partnumber BIGINT,
            operation BIGINT,
            slotnumber BIGINT,
            year INT,
            month INT
        )
        STORED AS PARQUET
        LOCATION '{batch_dir}'
        """
    )
    cur.execute(f"INVALIDATE METADATA {DB_NAME}.{STAGING_TBL}")
    cur.execute(f"REFRESH {DB_NAME}.{STAGING_TBL}")


def staging_count(cur) -> int:
    cur.execute(f"SELECT COUNT(*) FROM {DB_NAME}.{STAGING_TBL}")
    return int(cur.fetchall()[0][0])


def discover_affected_partitions(cur) -> List[Tuple[int, int, int, int]]:
    cur.execute(
        f"""
        SELECT DISTINCT year, month, partnumber, operation
        FROM {DB_NAME}.{STAGING_TBL}
        """
    )
    return [(int(y), int(m), int(pn), int(op)) for (y, m, pn, op) in cur.fetchall()]


def count_staging_partition(cur, y: int, m: int, pn: int, op: int) -> int:
    cur.execute(
        f"""
        SELECT COUNT(*) FROM {DB_NAME}.{STAGING_TBL}
        WHERE year={y} AND month={m} AND partnumber={pn} AND operation={op}
        """
    )
    return int(cur.fetchall()[0][0])


def count_target_partition(cur, y: int, m: int, pn: int, op: int) -> int:
    cur.execute(
        f"""
        SELECT COUNT(*) FROM {DB_NAME}.{TARGET_TBL}
        WHERE year={y} AND month={m} AND partnumber={pn} AND operation={op}
        """
    )
    return int(cur.fetchall()[0][0])


def drop_partition_everywhere(cur, table_location: str, y: int, m: int, pn: int, op: int) -> None:
    cur.execute(
        f"""
        ALTER TABLE {DB_NAME}.{TARGET_TBL}
        DROP IF EXISTS PARTITION (year={y}, month={m}, partnumber={pn}, operation={op})
        """
    )
    hdfs_rm_if_exists(partition_hdfs_path(table_location, y, m, pn, op), recursive=True)


def insert_into_partition(cur, y: int, m: int, pn: int, op: int, hint: str) -> None:
    cur.execute(
        f"""
        INSERT {hint}
        INTO TABLE {DB_NAME}.{TARGET_TBL}
        PARTITION (year={y}, month={m}, partnumber={pn}, operation={op})
        SELECT
            `timestamp`, f1, f2, f3, f4, ordernumber, slotnumber
        FROM {DB_NAME}.{STAGING_TBL}
        WHERE year={y} AND month={m} AND partnumber={pn} AND operation={op}
        """
    )


def overwrite_partition_from_target(cur, y: int, m: int, pn: int, op: int, hint: str) -> None:
    cur.execute(
        f"""
        INSERT {hint}
        OVERWRITE TABLE {DB_NAME}.{TARGET_TBL}
        PARTITION (year={y}, month={m}, partnumber={pn}, operation={op})
        SELECT
            `timestamp`, f1, f2, f3, f4, ordernumber, slotnumber
        FROM {DB_NAME}.{TARGET_TBL}
        WHERE year={y} AND month={m} AND partnumber={pn} AND operation={op}
        """
    )


def compute_stats(cur) -> None:
    cur.execute(f"COMPUTE STATS {DB_NAME}.{TARGET_TBL}")


def compute_incremental_stats_for_partition(cur, y: int, m: int, pn: int, op: int) -> None:
    cur.execute(
        f"""
        COMPUTE INCREMENTAL STATS {DB_NAME}.{TARGET_TBL}
        PARTITION (year={y}, month={m}, partnumber={pn}, operation={op})
        """
    )


def main() -> None:
    batch_dir = freeze_incoming_to_batch()
    if not batch_dir:
        print("No new files in incoming. Exiting.")
        return

    print("BATCH:", batch_dir)

    cur, conn = impala_on()
    try:
        cur.execute(f"USE {DB_NAME}")
        cur.execute(f"SET PARQUET_FILE_SIZE={PARQUET_FILE_SIZE}")
        cur.execute(f"SET MAX_FRAGMENT_INSTANCES_PER_NODE={MAX_FRAGMENT_INSTANCES_PER_NODE}")

        table_location = get_table_location(cur)
        print("TARGET LOCATION:", table_location)

        create_staging_for_batch(cur, batch_dir)
        cnt = staging_count(cur)
        print("staging_count =", cnt)
        if cnt == 0:
            return

        parts = discover_affected_partitions(cur)
        parts.sort()
        print("affected_partitions =", len(parts))

        hint = "/*+ SHUFFLE */" if USE_SHUFFLE_HINT else ""

        cur.execute(f"SET NUM_NODES={NUM_NODES_WRITE}")

        for (y, m, pn, op) in parts:
            new_rows = count_staging_partition(cur, y, m, pn, op)
            if new_rows == 0:
                continue

            part_path = partition_hdfs_path(table_location, y, m, pn, op)
            size_before = hdfs_du_bytes(part_path)

            print(f"APPEND y={y} m={m} pn={pn} op={op} new_rows={new_rows} size_before={size_before}")
            insert_into_partition(cur, y, m, pn, op, hint)

            size_after = hdfs_du_bytes(part_path)
            delta = max(0, size_after - size_before)

            print(f"  size_after={size_after} delta={delta}")

            if delta >= COMPACT_THRESHOLD_BYTES:
                print("  COMPACT -> OVERWRITE from TARGET")
                overwrite_partition_from_target(cur, y, m, pn, op, hint)

                after_cnt = count_target_partition(cur, y, m, pn, op)
                if after_cnt == 0:
                    drop_partition_everywhere(cur, table_location, y, m, pn, op)

        if DO_COMPUTE_INCREMENTAL_STATS or DO_COMPUTE_STATS:
            cur.execute(f"SET NUM_NODES={NUM_NODES_STATS}")

        if DO_COMPUTE_INCREMENTAL_STATS:
            for (y, m, pn, op) in parts:
                compute_incremental_stats_for_partition(cur, y, m, pn, op)

        if DO_COMPUTE_STATS:
            compute_stats(cur)

        cur.execute(f"INVALIDATE METADATA {DB_NAME}.{TARGET_TBL}")
        cur.execute(f"REFRESH {DB_NAME}.{TARGET_TBL}")

        processed_dir = archive_batch(batch_dir)
        print("ARCHIVED:", processed_dir)
        print("DONE.")

    finally:
        impala_off(cur, conn)


if __name__ == "__main__":
    main()
