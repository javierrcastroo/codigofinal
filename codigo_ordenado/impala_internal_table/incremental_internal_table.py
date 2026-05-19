"""Incremental loader for Impala internal tables by execution period.

This module appends new partitions or periods to previously created internal tables,
allowing recurring batch ingestion without full table rebuilds.
"""

from __future__ import annotations

from datetime import datetime
from typing import List, Tuple

from codigo_ordenado.utils.connection_to_db import impala_on, impala_off
from codigo_ordenado.utils.hdfs_utils import (
    hdfs_ls,
    hdfs_mkdir_p,
    hdfs_mv,
)

# =========================
# CONFIG
# =========================
DB_NAME = "sandbox_db"

# Landing zone where new parquet files arrive (written by NiFi/Kafka, etc.)
INCOMING_DIR = "/apps/impala/sandbox_db/data_auto/parquet_with_snappy_111_post_sintetico"

# Batch freeze + archive zones
BATCH_ROOT = "/apps/impala/sandbox_db/data_auto/batches/auto_sintetico"
PROCESSED_ROOT = "/apps/impala/sandbox_db/data_auto/batches_processed/auto_sintetico"

# Staging over the frozen batch
STAGING_TBL = "auto_staging_ext_sintetico_batch"

# Target table (MANAGED / INTERNAL table previously created)
TARGET_TBL = "auto_yyyy_mm_pn_op_sintetico_interno"

# Writer tuning
USE_SHUFFLE_HINT = False
PARQUET_FILE_SIZE = "64m"

NUM_NODES_WRITE = 1
NUM_NODES_STATS = 0
MAX_FRAGMENT_INSTANCES_PER_NODE: int | None = 1

# Stats
DO_COMPUTE_STATS = False
DO_COMPUTE_INCREMENTAL_STATS = True


def batch_id() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def incoming_has_data() -> bool:
    return len(hdfs_ls(INCOMING_DIR)) > 0


def freeze_incoming_to_batch() -> str:
    """
    Atomically move incoming/ -> batches/batch=YYYYMMDD_HHMMSS and recreate incoming/.
    This is the only place we touch HDFS besides archiving the batch.
    """
    if not incoming_has_data():
        return ""

    bid = batch_id()
    batch_dir = "{0}/batch={1}".format(BATCH_ROOT, bid)

    hdfs_mkdir_p(BATCH_ROOT)
    hdfs_mv(INCOMING_DIR, batch_dir)
    hdfs_mkdir_p(INCOMING_DIR)

    return batch_dir


def archive_batch(batch_dir: str) -> str:
    """
    Move processed batch to batches_processed/ so it won't be reprocessed.
    """
    processed_dir = batch_dir.replace(BATCH_ROOT, PROCESSED_ROOT)
    hdfs_mkdir_p(PROCESSED_ROOT)
    hdfs_mv(batch_dir, processed_dir)
    return processed_dir


def create_staging_for_batch(cur, batch_dir: str) -> None:
    """
    Create an EXTERNAL staging table over the frozen batch directory.
    """
    cur.execute("DROP TABLE IF EXISTS {0}.{1}".format(DB_NAME, STAGING_TBL))
    cur.execute(
        """
        CREATE EXTERNAL TABLE {db}.{stg} (
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
        LOCATION '{loc}'
        """.format(db=DB_NAME, stg=STAGING_TBL, loc=batch_dir)
    )

    # For a newly created external table, REFRESH is usually enough to pick up files.
    cur.execute("REFRESH {0}.{1}".format(DB_NAME, STAGING_TBL))


def staging_count(cur) -> int:
    cur.execute("SELECT COUNT(*) FROM {0}.{1}".format(DB_NAME, STAGING_TBL))
    return int(cur.fetchall()[0][0])


def discover_affected_partitions(cur) -> List[Tuple[int, int, int, int]]:
    """
    Find all (year, month, partnumber, operation) that appear in the current batch.
    """
    cur.execute(
        """
        SELECT DISTINCT year, month, partnumber, operation
        FROM {db}.{stg}
        """.format(db=DB_NAME, stg=STAGING_TBL)
    )
    return [(int(y), int(m), int(pn), int(op)) for (y, m, pn, op) in cur.fetchall()]


def count_staging_partition(cur, y: int, m: int, pn: int, op: int) -> int:
    cur.execute(
        """
        SELECT COUNT(*)
        FROM {db}.{stg}
        WHERE year={y} AND month={m} AND partnumber={pn} AND operation={op}
        """.format(db=DB_NAME, stg=STAGING_TBL, y=y, m=m, pn=pn, op=op)
    )
    return int(cur.fetchall()[0][0])


def overwrite_partition_union_target_and_staging(cur, y: int, m: int, pn: int, op: int, hint: str) -> None:
    """
    Variant B:
    One-pass overwrite that rewrites the whole partition as:
      (existing target rows) UNION ALL (new staging rows)

    Result: partition stays compact (few large parquet files), no small-file accumulation.
    """
    cur.execute(
        """
        INSERT {hint}
        OVERWRITE TABLE {db}.{tgt}
        PARTITION (year={y}, month={m}, partnumber={pn}, operation={op})

        SELECT
            `timestamp`, f1, f2, f3, f4, ordernumber, slotnumber
        FROM {db}.{tgt}
        WHERE year={y} AND month={m} AND partnumber={pn} AND operation={op}

        UNION ALL

        SELECT
            `timestamp`, f1, f2, f3, f4, ordernumber, slotnumber
        FROM {db}.{stg}
        WHERE year={y} AND month={m} AND partnumber={pn} AND operation={op}
        """.format(
            hint=hint,
            db=DB_NAME,
            tgt=TARGET_TBL,
            stg=STAGING_TBL,
            y=y,
            m=m,
            pn=pn,
            op=op,
        )
    )


def compute_stats(cur) -> None:
    cur.execute("COMPUTE STATS {0}.{1}".format(DB_NAME, TARGET_TBL))


def compute_incremental_stats_for_partition(cur, y: int, m: int, pn: int, op: int) -> None:
    cur.execute(
        """
        COMPUTE INCREMENTAL STATS {db}.{tgt}
        PARTITION (year={y}, month={m}, partnumber={pn}, operation={op})
        """.format(db=DB_NAME, tgt=TARGET_TBL, y=y, m=m, pn=pn, op=op)
    )


def main() -> None:
    batch_dir = freeze_incoming_to_batch()
    if not batch_dir:
        print("No new files in incoming. Exiting.")
        return

    print("BATCH:", batch_dir)

    cur, conn = impala_on()
    try:
        cur.execute("USE {0}".format(DB_NAME))
        cur.execute("SET PARQUET_FILE_SIZE={0}".format(PARQUET_FILE_SIZE))
        cur.execute("SET MAX_FRAGMENT_INSTANCES_PER_NODE={0}".format(MAX_FRAGMENT_INSTANCES_PER_NODE))

        create_staging_for_batch(cur, batch_dir)

        cnt = staging_count(cur)
        print("staging_count =", cnt)
        if cnt == 0:
            processed_dir = archive_batch(batch_dir)
            print("ARCHIVED (empty batch):", processed_dir)
            return

        parts = discover_affected_partitions(cur)
        parts.sort()
        print("affected_partitions =", len(parts))

        hint = "/*+ SHUFFLE */" if USE_SHUFFLE_HINT else ""

        # Reduce writers to avoid many small files (tune as needed)
        cur.execute("SET NUM_NODES={0}".format(NUM_NODES_WRITE))

        for (y, m, pn, op) in parts:
            new_rows = count_staging_partition(cur, y, m, pn, op)
            if new_rows == 0:
                continue

            print("OVERWRITE (union) y={0} m={1} pn={2} op={3} new_rows={4}".format(y, m, pn, op, new_rows))
            overwrite_partition_union_target_and_staging(cur, y, m, pn, op, hint)

        # Stats (optional)
        if DO_COMPUTE_INCREMENTAL_STATS or DO_COMPUTE_STATS:
            cur.execute("SET NUM_NODES={0}".format(NUM_NODES_STATS))

        if DO_COMPUTE_INCREMENTAL_STATS:
            for (y, m, pn, op) in parts:
                compute_incremental_stats_for_partition(cur, y, m, pn, op)

        if DO_COMPUTE_STATS:
            compute_stats(cur)

        # For MANAGED tables written by Impala, REFRESH/INVALIDATE is usually unnecessary.
        # Keep it off by default to avoid catalog pressure.
        # cur.execute("REFRESH {0}.{1}".format(DB_NAME, TARGET_TBL))

        processed_dir = archive_batch(batch_dir)
        print("ARCHIVED:", processed_dir)
        print("DONE.")

    finally:
        impala_off(cur, conn)


if __name__ == "__main__":
    main()
