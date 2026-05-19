"""Baseline internal-table build routine for Impala-managed datasets.

It creates and populates internal tables that downstream incremental loaders and
benchmark query suites depend on for stable performance testing.
"""

from __future__ import annotations

from codigo_ordenado.utils.connection_to_db import impala_on, impala_off

# =========================
# CONFIG
# =========================
DB_NAME = "sandbox_db"

SRC_PARQUET_DIR = "/apps/impala/sandbox_db/data_auto/parquet_with_snappy_111_sintetico"

STAGING_TBL = "auto_staging_ext_sintetico"
TARGET_TBL = "auto_yyyy_mm_pn_op_sintetico_interno"

YEARS = [2021, 2022, 2023, 2024, 2025]
MONTHS = list(range(1, 13))

PARQUET_FILE_SIZE = "64m"


def main() -> None:
    cur, conn = impala_on()
    try:
        # 1) DB
        cur.execute("CREATE DATABASE IF NOT EXISTS {0}".format(DB_NAME))
        cur.execute("USE {0}".format(DB_NAME))

        # 2) Drop tables
        cur.execute("DROP TABLE IF EXISTS {0}.{1}".format(DB_NAME, STAGING_TBL))
        cur.execute("DROP TABLE IF EXISTS {0}.{1}".format(DB_NAME, TARGET_TBL))

        # 3) STAGING (external over SRC)
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
            """.format(db=DB_NAME, stg=STAGING_TBL, src=SRC_PARQUET_DIR)
        )

        # Make Impala pick up files in the external LOCATION
        # REFRESH is usually sufficient; INVALIDATE is heavier.
        cur.execute("REFRESH {0}.{1}".format(DB_NAME, STAGING_TBL))

        # 4) TARGET (MANAGED / INTERNAL) partitioned
        cur.execute(
            """
            CREATE TABLE {db}.{tgt} (
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
            TBLPROPERTIES ('parquet.compression'='SNAPPY')
            """.format(db=DB_NAME, tgt=TARGET_TBL)
        )

        # Optional: aim for ~64MB parquet files (not guaranteed)
        cur.execute("SET PARQUET_FILE_SIZE={0}".format(PARQUET_FILE_SIZE))

        # Optional: avoid too much parallelism in small clusters (tune as needed)
        # cur.execute("SET NUM_NODES=1")

        # 5) INSERT chunked by (year, month)
        for y in YEARS:
            for m in MONTHS:
                print("INSERT year={0} month={1}".format(y, m))
                cur.execute(
                    """
                    INSERT INTO TABLE {db}.{tgt}
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
                    WHERE year = {y} AND month = {m}
                    """.format(
                        db=DB_NAME,
                        tgt=TARGET_TBL,
                        stg=STAGING_TBL,
                        y=y,
                        m=m,
                    )
                )

        # 6) Final metadata sync
        # For managed tables written by Impala, this is usually unnecessary.
        # Leave it commented to avoid catalog pressure.
        # cur.execute("REFRESH {0}.{1}".format(DB_NAME, TARGET_TBL))

        print("DONE.")

    finally:
        impala_off(cur, conn)


if __name__ == "__main__":
    main()
