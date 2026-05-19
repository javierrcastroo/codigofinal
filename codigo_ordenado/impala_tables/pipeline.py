"""End-to-end orchestration pipeline for Impala table lifecycle operations.

This module coordinates preparation, loading, and execution order across related
Impala scripts to provide repeatable enterprise batch processing.
"""

from __future__ import annotations

import random
import time
from typing import Tuple

from codigo_ordenado.impala_tables import post_sintetico
from codigo_ordenado.impala_tables import post_aver

from codigo_ordenado.utils.hdfs_utils import hdfs_ls


# ============================================================
# CONFIG
# ============================================================

ITERS = 12

SEED = 42

MIN_FILES = 20
MAX_FILES = 150

PN_START = 14000000

YEAR = 2026
MONTH = 1

INCOMING_DIR = "/apps/impala/sandbox_db/data_auto/parquet_with_snappy_111_post_sintetico"

SLEEP_SEC = 0.0


# ============================================================
# PN schedule
# ============================================================

def pn_range(iter_idx: int) -> Tuple[int, int]:

    block = (iter_idx - 1) // 2

    low = PN_START + ((block + 1) // 2)

    high = low + (1 if block % 2 == 0 else 0)

    return low, high


# ============================================================
# main
# ============================================================

def main() -> None:

    rng = random.Random(SEED)

    print()
    print("PIPELINE START")
    print()

    for it in range(1, ITERS + 1):

        pn_min, pn_max = pn_range(it)

        num_files = rng.randint(MIN_FILES, MAX_FILES)

        print("--------------------------------------------------")
        print(f"ITER {it}/{ITERS}")
        print(f"PN range = [{pn_min}, {pn_max}]")
        print(f"files    = {num_files}")
        print("--------------------------------------------------")

        # ----------------------------------------------------
        # Safety check
        # ----------------------------------------------------

        if hdfs_ls(INCOMING_DIR):

            raise RuntimeError("incoming dir not empty")

        # ----------------------------------------------------
        # GENERATE FILES
        # ----------------------------------------------------

        print("GENERATING FILES...")

        post_sintetico.HDFS_DIR = INCOMING_DIR
        post_sintetico.NUM_FILES = num_files

        post_sintetico.SEED = SEED + it * 100

        post_sintetico.PN_MIN = pn_min
        post_sintetico.PN_MAX = pn_max

        post_sintetico.YEAR_MIN = YEAR
        post_sintetico.YEAR_MAX = YEAR

        post_sintetico.MONTH_MIN = MONTH
        post_sintetico.MONTH_MAX = MONTH

        post_sintetico.main()

        created = len(hdfs_ls(INCOMING_DIR))

        print("created:", created)

        # ----------------------------------------------------
        # LOAD INTO IMPALA
        # ----------------------------------------------------

        print("LOADING INTO IMPALA...")

        post_aver.INCOMING_DIR = INCOMING_DIR

        post_aver.main()

        remaining = len(hdfs_ls(INCOMING_DIR))

        print("remaining:", remaining)

        if SLEEP_SEC > 0:

            time.sleep(SLEEP_SEC)

    print()
    print("PIPELINE FINISHED")


# ============================================================

if __name__ == "__main__":

    main()
