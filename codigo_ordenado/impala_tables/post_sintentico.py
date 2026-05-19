#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Post-processing pipeline for synthetic Impala data assets.

After synthetic generation, this script applies final transformations and load
steps so benchmark suites can run on a consistent target structure.
"""


from __future__ import annotations

import math
import random
import subprocess
import tempfile
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import Tuple, List

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


# ============================================================
# CONFIG
# ============================================================

HDFS_DIR = "/apps/impala/sandbox_db/data_auto/parquet_with_snappy_111_post_sintetico"

NUM_FILES = 20

MIN_KB = 600
MAX_KB = 1024

COMPRESSION = "snappy"

SEED = 42

PREFIX = "synthetic"

# ---- STRICT RANGES ----

PN_MIN = 14000003
PN_MAX = 14000003

OF_MIN = 22000000
OF_MAX = 22000007

OP_MIN = 1
OP_MAX = 2

SL_MIN = 1
SL_MAX = 16

YEAR_MIN = 2026
YEAR_MAX = 2026

MONTH_MIN = 1
MONTH_MAX = 1

# ---- timing ----

STEP = 0.0002

# ---- performance ----

BATCH_SIZE = 500
N_PROCESSES = max(1, min(16, cpu_count()))

ROW_GROUP_SIZE = 64_000

USE_DICTIONARY = False
WRITE_STATISTICS = False

PARQUET_VERSION = "1.0"

PROGRESS_EVERY_BATCHES = 10


# ============================================================
# HDFS helpers
# ============================================================

def _run(cmd: List[str]) -> Tuple[int, str, str]:

    p = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    out, err = p.communicate()

    return p.returncode, (out or "").strip(), (err or "").strip()


def hdfs_mkdir_p(path: str) -> None:

    code, _, err = _run(["hdfs", "dfs", "-mkdir", "-p", path])

    if code != 0:

        raise RuntimeError(err)


# ============================================================
# Parquet writer (Impala compatible)
# ============================================================

def write_parquet_impala(table: pa.Table, out_path: str) -> None:

    with pq.ParquetWriter(
        out_path,
        table.schema,
        compression=COMPRESSION,
        version=PARQUET_VERSION,
        use_dictionary=USE_DICTIONARY,
        write_statistics=WRITE_STATISTICS,
    ) as writer:

        writer.write_table(
            table,
            row_group_size=ROW_GROUP_SIZE,
        )


# ============================================================
# random helpers
# ============================================================

def _round2(x: np.ndarray) -> np.ndarray:

    return np.round(x, 2)


def _rand_int(rng: random.Random, lo: int, hi: int) -> int:

    return rng.randint(lo, hi)


# ============================================================
# build plan
# ============================================================

def _make_rows_plan(
    rng: random.Random,
    total_rows: int,
    op_count: int,
):

    sl_max_list = [
        _rand_int(rng, SL_MIN, SL_MAX)
        for _ in range(op_count)
    ]
    group_count = sum(sl_max_list)

    if total_rows < group_count:
        new_list = []
        remaining = total_rows
        
        for sl_max in sl_max_list:
            if remaining <= 0:
                break
            take = min(sl_max, remaining)
            new_list.append(take)
            remaining -= take
        sl_max_list = new_list
        group_count = sum(sl_max_list)
        op_count = len(sl_max_list)
    base = np.ones(group_count, dtype=np.int64)
    extra = total_rows - group_count

    if extra > 0:
        probs = np.ones(group_count) / group_count
        add = np.random.multinomial(extra, probs)
        counts = base + add

    else:
        counts = base

    op_vals = []
    sl_vals = []
    ts_vals = []
    idx = 0

    for op_idx in range(op_count):
        op = op_idx + 1
        sl_max = sl_max_list[op_idx]
        t0 = 0.0

        for sl in range(1, sl_max + 1):
            n = int(counts[idx])
            idx += 1
            op_vals.append(
                np.full(n, op, dtype=np.int64)
            )
            sl_vals.append(
                np.full(n, sl, dtype=np.int64)
            )
            ts = t0 + STEP * np.arange(n)
            ts_vals.append(ts)

            if n > 0:
                t0 = ts[-1] + STEP

    operation_arr = np.concatenate(op_vals)
    slot_arr = np.concatenate(sl_vals)
    ts_arr = np.concatenate(ts_vals)

    if len(operation_arr) != total_rows:
        operation_arr = operation_arr[:total_rows]
        slot_arr = slot_arr[:total_rows]
        ts_arr = ts_arr[:total_rows]
    return operation_arr, slot_arr, ts_arr


# ============================================================
# build table
# ============================================================

def _build_table(
    rng: random.Random,
    total_rows: int,
) -> pa.Table:
  
    partnumber = _rand_int(rng, PN_MIN, PN_MAX)
    ordernumber = _rand_int(rng, OF_MIN, OF_MAX)
    year = _rand_int(rng, YEAR_MIN, YEAR_MAX)
    month = _rand_int(rng, MONTH_MIN, MONTH_MAX)
    op_count = _rand_int(rng, OP_MIN, OP_MAX)
    operation_arr, slot_arr, ts_arr = _make_rows_plan(
        rng,
        total_rows,
        op_count,
    )

    f1 = _round2(
        np.random.uniform(-3000, 3000, total_rows)
    )
    f2 = _round2(
        np.random.uniform(-3000, 3000, total_rows)
    )
    f3 = _round2(
        np.random.uniform(-3000, 3000, total_rows)
    )
    f4 = _round2(
        np.random.uniform(-3000, 3000, total_rows)
    )
    pn_arr = np.full(total_rows, partnumber, dtype=np.int64)
    of_arr = np.full(total_rows, ordernumber, dtype=np.int64)
    year_arr = np.full(total_rows, year, dtype=np.int32)
    month_arr = np.full(total_rows, month, dtype=np.int32)
    schema = pa.schema([
        ("timestamp", pa.float64()),
        ("f1", pa.float64()),
        ("f2", pa.float64()),
        ("f3", pa.float64()),
        ("f4", pa.float64()),
        ("ordernumber", pa.int64()),
        ("partnumber", pa.int64()),
        ("operation", pa.int64()),
        ("slotnumber", pa.int64()),
        ("year", pa.int32()),
        ("month", pa.int32()),
    ])
    return pa.Table.from_arrays(
        [
            pa.array(ts_arr),
            pa.array(f1),
            pa.array(f2),
            pa.array(f3),
            pa.array(f4),
            pa.array(of_arr),
            pa.array(pn_arr),
            pa.array(operation_arr),
            pa.array(slot_arr),
            pa.array(year_arr),
            pa.array(month_arr),
        ],
        schema=schema,
    )


# ============================================================
# estimate size
# ============================================================

def _estimate_bytes_per_row(seed: int) -> float:
    rng = random.Random(seed)
    np.random.seed(seed)
    rows = 20000
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "cal.parquet"
        table = _build_table(rng, rows)
        write_parquet_impala(table, str(path))
        size = path.stat().st_size
    return size / rows


# ============================================================
# batch worker
# ============================================================

def _write_batch(args):
    batch_idx, start_i, end_i, bytes_per_row, seed = args
    mix = (seed + batch_idx * 10000019) % (2**32)
    rng = random.Random(mix)
    np.random.seed(mix)
    min_b = MIN_KB * 1024
    max_b = MAX_KB * 1024
    local_dir = Path(
        tempfile.mkdtemp(
            prefix=f"pq_batch_{batch_idx:06d}_"
        )
    )

    try:
        for i in range(start_i, end_i):
            target = _rand_int(rng, min_b, max_b)
            rows = max(
                10,
                int(math.ceil(target / bytes_per_row)),
            )
            table = _build_table(rng, rows)
            out_path = local_dir / f"{PREFIX}_{i:09d}.parquet"
            write_parquet_impala(
                table,
                str(out_path),
            )
        files = sorted(
            str(p)
            for p in local_dir.glob("*.parquet")
        )
        if files:
            code, _, err = _run(
                ["hdfs", "dfs", "-put", "-f"]
                + files
                + [HDFS_DIR]
            )
            if code != 0:
                raise RuntimeError(err)
        return end_i - start_i

    finally:
        for p in local_dir.glob("*"):
            try:
                p.unlink()
            except:
                pass
        try:
            local_dir.rmdir()
        except:
            pass


# ============================================================
# main
# ============================================================

def main():
    hdfs_mkdir_p(HDFS_DIR)
    bytes_per_row = _estimate_bytes_per_row(SEED)
    total_batches = math.ceil(NUM_FILES / BATCH_SIZE)
    tasks = []

    for b in range(total_batches):
        start = b * BATCH_SIZE
        end = min(NUM_FILES, (b + 1) * BATCH_SIZE)
        tasks.append(
            (b, start, end, bytes_per_row, SEED)
        )
    done = 0
    with Pool(N_PROCESSES) as pool:
        for i, created in enumerate(
            pool.imap_unordered(_write_batch, tasks),
            start=1,
        ):
            done += created
            if i % PROGRESS_EVERY_BATCHES == 0:
                print(
                    f"[batches {i}/{total_batches}] files={done}"
                )

    print()
    print("DONE")
    print("files:", done)
    print("path:", HDFS_DIR)


# ============================================================

if __name__ == "__main__":

    main()
