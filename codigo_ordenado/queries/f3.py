from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from codigo_ordenado.utils.connection_to_db import impala_on, impala_off, hiveql_on, hiveql_off
from codigo_ordenado.utils.hdfs_utils import hdfs_mkdir_p, hdfs_put_file

import codigo_ordenado.queries.f2 as qi
import codigo_ordenado.queries.f1 as qh


def _load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _hive_open():
    curs, conn = hiveql_on()
    qh.hive_apply_session_settings(curs)
    return curs, conn


def _impala_open():
    curs, conn = impala_on()
    qi.impala_apply_session_settings(curs)
    return curs, conn


def _engine_parts(engine: str):
    if engine == "hive":
        return (
            _hive_open,
            hiveql_off,
            qh,
            qh.hive_exec,
        )
    if engine == "impala":
        return (
            _impala_open,
            impala_off,
            qi,
            qi.impala_exec,
        )
    raise ValueError("engine must be hive|impala")


def hive_table_exists(curs, db: str, table: str) -> bool:
    curs.execute("SHOW TABLES IN {} LIKE '{}'".format(db, table))
    return bool(curs.fetchall())


def impala_table_exists(curs, db: str, table: str) -> bool:
    curs.execute("SHOW TABLES IN {} LIKE '{}'".format(db, table))
    return bool(curs.fetchall())


def _build_query_plan(
    module,
    table: str,
    cols: Dict[str, str],
    top_pn_all: int,
    top_pn_last6: Optional[int],
    it: int,
    query_names: List[str],
):
    plan: List[Tuple[str, str]] = []

    for qname in query_names:
        if "last6" in qname and top_pn_last6 is None:
            continue

        if qname == "Q1_all":
            plan.append((qname, module.sql_q1_all(table, cols)))
        elif qname == "Q1_last6":
            plan.append((qname, module.sql_q1_last6(table, cols)))

        elif qname == "Q2_all_add":
            plan.append((qname, module.sql_q2_all_add(table, cols, top_pn_all)))
        elif qname == "Q2_all_add_rand":
            plan.append((qname, module.sql_q2_all_add_rand(table, cols, top_pn_all, it)))
        elif qname == "Q2_all_sub":
            plan.append((qname, module.sql_q2_all_sub(table, cols, top_pn_all)))
        elif qname == "Q2_all_sub_rand":
            plan.append((qname, module.sql_q2_all_sub_rand(table, cols, top_pn_all, it)))

        elif qname == "Q3_all_add":
            plan.append((qname, module.sql_q3_all_add(table, cols, top_pn_all)))
        elif qname == "Q3_all_add_rand":
            plan.append((qname, module.sql_q3_all_add_rand(table, cols, top_pn_all, it)))
        elif qname == "Q3_all_sub":
            plan.append((qname, module.sql_q3_all_sub(table, cols, top_pn_all)))
        elif qname == "Q3_all_sub_rand":
            plan.append((qname, module.sql_q3_all_sub_rand(table, cols, top_pn_all, it)))

        elif qname == "Q2_last6_add":
            plan.append((qname, module.sql_q2_last6_add(table, cols, int(top_pn_last6))))
        elif qname == "Q2_last6_add_rand":
            plan.append((qname, module.sql_q2_last6_add_rand(table, cols, int(top_pn_last6), it)))
        elif qname == "Q2_last6_sub":
            plan.append((qname, module.sql_q2_last6_sub(table, cols, int(top_pn_last6))))
        elif qname == "Q2_last6_sub_rand":
            plan.append((qname, module.sql_q2_last6_sub_rand(table, cols, int(top_pn_last6), it)))

        elif qname == "Q3_last6_add":
            plan.append((qname, module.sql_q3_last6_add(table, cols, int(top_pn_last6))))
        elif qname == "Q3_last6_add_rand":
            plan.append((qname, module.sql_q3_last6_add_rand(table, cols, int(top_pn_last6), it)))
        elif qname == "Q3_last6_sub":
            plan.append((qname, module.sql_q3_last6_sub(table, cols, int(top_pn_last6))))
        elif qname == "Q3_last6_sub_rand":
            plan.append((qname, module.sql_q3_last6_sub_rand(table, cols, int(top_pn_last6), it)))

        else:
            raise ValueError("Unknown query name: {}".format(qname))

    return plan


def _precompute_schema_map(engine: str, tables: List[str], db_name: str) -> Dict[str, Dict[str, str]]:
    """
    Build {table: {"pn": ..., "op": ..., "sl": ...}} OUTSIDE timed sections.
    """
    open_fn, close_fn, module, _exec_fn = _engine_parts(engine)

    schema_map: Dict[str, Dict[str, str]] = {}
    curs, conn = open_fn()
    try:
        for table in tables:
            # Skip missing tables (leave unmapped)
            if engine == "hive":
                ok = hive_table_exists(curs, db_name, table)
            else:
                ok = impala_table_exists(curs, db_name, table)

            if not ok:
                continue

            schema_map[table] = module.detect_table_schema(curs, table)
    finally:
        close_fn(curs, conn)

    return schema_map


def run_engine(
    engine: str,
    tables: List[str],
    exec_mode: str,
    iterations: int,
    query_names: List[str],
    db_name: str,
    schema_map: Dict[str, Dict[str, str]],
) -> List[Dict[str, Any]]:
    assert engine in ("impala", "hive")
    assert exec_mode in ("per_query", "per_iteration")

    rows: List[Dict[str, Any]] = []

    open_fn, close_fn, module, exec_fn = _engine_parts(engine)

    for table in tables:
        # Preflight: skip missing tables
        curs, conn = open_fn()
        try:
            if engine == "hive":
                ok = hive_table_exists(curs, db_name, table)
            else:
                ok = impala_table_exists(curs, db_name, table)
        finally:
            close_fn(curs, conn)

        if not ok:
            print("[SKIP] {} table not found: {}.{}".format(engine, db_name, table))
            rows.append({
                "engine": engine,
                "exec_mode": exec_mode,
                "table": table,
                "iteration": None,
                "query": None,
                "seconds": None,
                "status": "missing_table",
                "pn_col": None,
                "op_col": None,
                "sl_col": None,
            })
            continue

        cols = schema_map.get(table)
        if cols is None:
            # Should not happen because we precomputed, but keep safe.
            curs, conn = open_fn()
            try:
                cols = module.detect_table_schema(curs, table)
            finally:
                close_fn(curs, conn)
            schema_map[table] = cols

        for it in range(1, iterations + 1):

            if exec_mode == "per_iteration":
                curs, conn = open_fn()
                try:
                    top_pn_all, _ = module.get_top_partnumber_all_years_with_cursor(curs, table, cols)

                    top6 = module.get_top_partnumber_months_filter_with_cursor(curs, table, cols)
                    top_pn_6m: Optional[int] = None
                    if top6 is not None:
                        top_pn_6m, _ = top6

                    plan = _build_query_plan(module, table, cols, top_pn_all, top_pn_6m, it, query_names)

                    for qname, sql in plan:
                        dt, _ = exec_fn(curs, sql)
                        rows.append({
                            "engine": engine,
                            "exec_mode": exec_mode,
                            "table": table,
                            "iteration": it,
                            "query": qname,
                            "seconds": dt,
                            "status": "ok",
                            "pn_col": cols["pn"],
                            "op_col": cols["op"],
                            "sl_col": cols["sl"],
                        })
                finally:
                    close_fn(curs, conn)

            else:
                # per_query: compute tops once per iteration
                curs, conn = open_fn()
                try:
                    top_pn_all, _ = module.get_top_partnumber_all_years_with_cursor(curs, table, cols)

                    top6 = module.get_top_partnumber_months_filter_with_cursor(curs, table, cols)
                    top_pn_6m: Optional[int] = None
                    if top6 is not None:
                        top_pn_6m, _ = top6
                finally:
                    close_fn(curs, conn)

                plan = _build_query_plan(module, table, cols, top_pn_all, top_pn_6m, it, query_names)

                for qname, sql in plan:
                    curs, conn = open_fn()
                    try:
                        dt, _ = exec_fn(curs, sql)
                        rows.append({
                            "engine": engine,
                            "exec_mode": exec_mode,
                            "table": table,
                            "iteration": it,
                            "query": qname,
                            "seconds": dt,
                            "status": "ok",
                            "pn_col": cols["pn"],
                            "op_col": cols["op"],
                            "sl_col": cols["sl"],
                        })
                    finally:
                        close_fn(curs, conn)

    return rows


def main():
    cfg_path = os.environ.get("BENCH_CFG", "codigo_ordenado/config/benchmark_forst2.json")
    cfg = _load_json(cfg_path)

    db_name = cfg.get("db_name", "sandbox_db")
    iterations = int(cfg.get("iterations", 4))
    exec_modes = cfg.get("exec_modes", ["per_query"])
    engines = cfg.get("engines", ["hive", "impala"])
    query_names = cfg.get("queries", [])

    tables_cfg = cfg.get("tables", {})
    hive_tables = tables_cfg.get("hive", [])
    impala_tables = tables_cfg.get("impala", [])

    out_cfg = cfg.get("outputs", {})
    local_dir = out_cfg.get("local_dir", "/tmp")
    hdfs_dir = out_cfg.get("hdfs_dir", "/apps/hive/sandbox_db/benchmarks/forst2")
    csv_name = out_cfg.get("csv_name", "benchmark_forst2.csv")

    if not query_names:
        raise ValueError("Config must include a non-empty 'queries' list")

    hdfs_mkdir_p(hdfs_dir)

    all_rows: List[Dict[str, Any]] = []

    # Precompute schema maps OUTSIDE timing
    hive_schema_map = _precompute_schema_map("hive", hive_tables, db_name)
    impala_schema_map = _precompute_schema_map("impala", impala_tables, db_name)

    for engine in engines:
        tables = hive_tables if engine == "hive" else impala_tables
        schema_map = hive_schema_map if engine == "hive" else impala_schema_map

        for exec_mode in exec_modes:
            all_rows += run_engine(
                engine=engine,
                tables=tables,
                exec_mode=exec_mode,
                iterations=iterations,
                query_names=query_names,
                db_name=db_name,
                schema_map=schema_map,
            )

    df = pd.DataFrame(all_rows)

    local_csv = os.path.join(local_dir, csv_name)
    df.to_csv(local_csv, index=False)

    hdfs_put_file(local_csv, "{}/{}".format(hdfs_dir, csv_name))
    print("[DONE] Wrote:", local_csv)
    print("[DONE] HDFS:", "{}/{}".format(hdfs_dir, csv_name))


if __name__ == "__main__":
    main()
