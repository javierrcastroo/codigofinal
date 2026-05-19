"""Year-specific Impala loading workflow for 2026 partition scenarios.

The script encapsulates targeted logic for year-partitioned synthetic tables that
feed enterprise capacity and performance simulations.
"""

from __future__ import annotations

from codigo_ordenado.utils.connection_to_db import impala_on, impala_off

DB_NAME = "sandbox_db"
TARGET_TBL = "auto_yyyy_mm_pn_op_sintetico"


def main() -> None:

    cur, conn = impala_on()

    try:
        cur.execute(f"USE {DB_NAME}")

        cur.execute(f"SHOW PARTITIONS {DB_NAME}.{TARGET_TBL}")

        rows = cur.fetchall()

        to_drop = []

        for r in rows:
            p = r[0]
            if not p.startswith("year=2026/"):
                continue

            kv = dict(seg.split("=") for seg in p.split("/"))

            y = kv["year"]
            m = kv["month"]
            pn = kv["partnumber"]
            op = kv["operation"]

            to_drop.append((y, m, pn, op))

        print("Dropping", len(to_drop), "partitions")

        for y, m, pn, op in to_drop:

            print(f"Dropping year={y}, month={m}, pn={pn}, op={op}")

            cur.execute(
                f"""
                ALTER TABLE {DB_NAME}.{TARGET_TBL}
                DROP PARTITION (year={y}, month={m}, partnumber={pn}, operation={op})
                """
            )

        cur.execute(f"INVALIDATE METADATA {DB_NAME}.{TARGET_TBL}")
        cur.execute(f"REFRESH {DB_NAME}.{TARGET_TBL}")

        print("DONE")

    finally:
        impala_off(cur, conn)


if __name__ == "__main__":
    main()
