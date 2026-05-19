"""Validation and sandbox test operations for Impala table management.

It contains controlled test routines used to verify table DDL/DML behavior and HDFS
integration before promoting workflow changes to broader runs.
"""

from __future__ import annotations

from codigo_ordenado.utils.connection_to_db import impala_on, impala_off
from codigo_ordenado.utils.hdfs_utils import hdfs_rm_if_exists

# ============================================================
# CONFIG
# ============================================================

DB_NAME = "sandbox_db"

SRC_PARQUET_DIR = "/apps/impala/sandbox_db/data_auto/parquet_with_snappy_111_sintetico"
DEST_PARQUET_DIR = "/apps/impala/sandbox_db/data/parquet_auto/auto_yyyy_mm_pn_op_spark"

STAGING_TBL = "auto_staging_ext_sintetico"
TARGET_TBL = "auto_yyyy_mm_pn_op_sintetico"

# Point 5: avoid COMPUTE STATS during iterative runs
DO_COMPUTE_STATS = False


def main() -> None:
    # Clean destination output (external table location)
    hdfs_rm_if_exists(DEST_PARQUET_DIR, recursive=True)

    cur, conn = impala_on()
    try:
        # ----------------------------------------------------
        # DB
        # ----------------------------------------------------
        cur.execute("CREATE DATABASE IF NOT EXISTS {0}".format(DB_NAME))
        cur.execute("USE {0}".format(DB_NAME))

        # ----------------------------------------------------
        # DROP TABLES
        # ----------------------------------------------------
        cur.execute("DROP TABLE IF EXISTS {0}.{1}".format(DB_NAME, STAGING_TBL))
        cur.execute("DROP TABLE IF EXISTS {0}.{1}".format(DB_NAME, TARGET_TBL))

        # ----------------------------------------------------
        # STAGING
        # ----------------------------------------------------
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
            LOCATION '{src}'
            """.format(
                db=DB_NAME,
                stg=STAGING_TBL,
                src=SRC_PARQUET_DIR,
            )
        )

        # Make sure Impala sees the source files
        cur.execute("INVALIDATE METADATA {0}.{1}".format(DB_NAME, STAGING_TBL))
        cur.execute("REFRESH {0}.{1}".format(DB_NAME, STAGING_TBL))

        # Sanity check (fast, prevents 1h waiting for nothing)
        cur.execute("SELECT COUNT(*) FROM {0}.{1}".format(DB_NAME, STAGING_TBL))
        stg_cnt = cur.fetchall()[0][0]
        print("staging_count =", stg_cnt)
        if stg_cnt == 0:
            print("ERROR: staging reads 0 rows from Impala. Aborting.")
            return

        # ----------------------------------------------------
        # TARGET (PARTITIONED)
        # ----------------------------------------------------
        cur.execute(
            """
            CREATE EXTERNAL TABLE {db}.{tgt} (
                `timestamp` DOUBLE,
                f1 DOUBLE,
                f2 DOUBLE,
                f3 DOUBLE,
                f4 DOUBLE,
                ordernumber BIGINT,
                slotnumber BIGINT
            )
            PARTITIONED BY (
                year INT,
                month INT,
                partnumber BIGINT,
                operation BIGINT
            )
            STORED AS PARQUET
            LOCATION '{dest}'
            TBLPROPERTIES ('parquet.compression'='SNAPPY')
            """.format(
                db=DB_NAME,
                tgt=TARGET_TBL,
                dest=DEST_PARQUET_DIR,
            )
        )

        # Optional hint for output files
        cur.execute("SET PARQUET_FILE_SIZE=64m")

        # ----------------------------------------------------
        # INSERT (COMPACTION + PARTITIONING)
        # ----------------------------------------------------
        cur.execute(
            """
            INSERT /*+ SHUFFLE */
            INTO TABLE {db}.{tgt}
            PARTITION (year, month, partnumber, operation)
            SELECT
                `timestamp`,
                f1,
                f2,
                f3,
                f4,
                ordernumber,
                slotnumber,
                year,
                month,
                partnumber,
                operation
            FROM {db}.{stg}
            """.format(
                db=DB_NAME,
                tgt=TARGET_TBL,
                stg=STAGING_TBL,
            )
        )

        # ----------------------------------------------------
        # METADATA
        # ----------------------------------------------------
        cur.execute("INVALIDATE METADATA {0}.{1}".format(DB_NAME, TARGET_TBL))
        cur.execute("REFRESH {0}.{1}".format(DB_NAME, TARGET_TBL))

        # Point 5: skip COMPUTE STATS unless explicitly enabled
        if DO_COMPUTE_STATS:
            cur.execute("COMPUTE STATS {0}.{1}".format(DB_NAME, TARGET_TBL))
        else:
            print("Skipping COMPUTE STATS (DO_COMPUTE_STATS=False)")

    finally:
        impala_off(cur, conn)


if __name__ == "__main__":
    main()
