"""Focused benchmark scenario F1 for representative analytical query patterns.

The module encapsulates one workload family so results can be compared independently
within the broader benchmark orchestrator and reporting process.
"""

from __future__ import annotations

from time import perf_counter
from typing import Any, Dict, List, Optional, Tuple

DB_NAME = "sandbox_db"

# "last6" now means: these months across ALL years (not tied to 2024).
MONTHS_FILTER = (7, 8, 9, 10, 11, 12)


def hive_apply_session_settings(curs) -> None:
    # Best-effort knobs
    for stmt in [
        "set hive.query.results.cache.enabled=false",
        "set hive.vectorized.execution.enabled=false",
    ]:
        try:
            curs.execute(stmt)
        except Exception:
            pass


def hive_exec(curs, sql: str) -> Tuple[float, List[Tuple[Any, ...]]]:
    t0 = perf_counter()
    curs.execute(sql)
    res = curs.fetchall()
    return perf_counter() - t0, res


def _months_list() -> str:
    return ", ".join(str(m) for m in MONTHS_FILTER)


# ============================================================
# Schema detection (run OUTSIDE timed sections)
# ============================================================
def detect_table_schema(curs, table: str) -> Dict[str, str]:
    """
    Detect column names for pn/op/sl for this table.

    Typed-like tables:
      partnumber, operation, slotnumber

    Clean_nov tables:
      pn, op, sl

    Output:
      {"pn": <col>, "op": <col>, "sl": <col>}
    """
    curs.execute("DESCRIBE {}.{}".format(DB_NAME, table))
    # Hive DESCRIBE returns rows like: (col_name, data_type, comment) and also meta sections.
    cols: List[str] = []
    for r in curs.fetchall():
        if not r:
            continue
        name = (r[0] or "").strip()
        if not name:
            continue
        if name.startswith("#"):
            continue
        # Skip partition header rows or empty separators
        if name.lower() in ("partition information",):
            continue
        cols.append(name.lower())

    # Prefer explicit columns
    if "partnumber" in cols:
        pn_col = "partnumber"
    elif "pn" in cols:
        pn_col = "pn"
    else:
        raise ValueError("No pn/partnumber column found in {}.{}".format(DB_NAME, table))

    if "operation" in cols:
        op_col = "operation"
    elif "op" in cols:
        op_col = "op"
    else:
        raise ValueError("No op/operation column found in {}.{}".format(DB_NAME, table))

    if "slotnumber" in cols:
        sl_col = "slotnumber"
    elif "sl" in cols:
        sl_col = "sl"
    else:
        raise ValueError("No sl/slotnumber column found in {}.{}".format(DB_NAME, table))

    return {"pn": pn_col, "op": op_col, "sl": sl_col}


# ----------------------------
# TOP partnumber (dynamic cols)
# ----------------------------
def sql_get_top_partnumber_all_years(table: str, cols: Dict[str, str]) -> str:
    pn = cols["pn"]
    return """
    SELECT {pn} AS partnumber, COUNT(*) AS numrows
    FROM {db}.{table}
    GROUP BY {pn}
    ORDER BY numrows DESC
    LIMIT 1
    """.format(pn=pn, db=DB_NAME, table=table).strip()


def sql_get_top_partnumber_months_filter(table: str, cols: Dict[str, str]) -> str:
    pn = cols["pn"]
    months = _months_list()
    return """
    SELECT {pn} AS partnumber, COUNT(*) AS numrows
    FROM {db}.{table}
    WHERE month IN ({months})
    GROUP BY {pn}
    ORDER BY numrows DESC
    LIMIT 1
    """.format(pn=pn, db=DB_NAME, table=table, months=months).strip()


# ----------------------------
# Q1
# ----------------------------
def sql_q1_all(table: str, cols: Dict[str, str]) -> str:
    pn = cols["pn"]
    op = cols["op"]
    return """
    SELECT {pn} AS partnumber, {op} AS operation, COUNT(*) AS numrows
    FROM {db}.{table}
    GROUP BY {pn}, {op}
    ORDER BY numrows DESC
    """.format(pn=pn, op=op, db=DB_NAME, table=table).strip()


def sql_q1_last6(table: str, cols: Dict[str, str]) -> str:
    pn = cols["pn"]
    op = cols["op"]
    months = _months_list()
    return """
    SELECT {pn} AS partnumber, {op} AS operation, COUNT(*) AS numrows
    FROM {db}.{table}
    WHERE month IN ({months})
    GROUP BY {pn}, {op}
    ORDER BY numrows DESC
    """.format(pn=pn, op=op, db=DB_NAME, table=table, months=months).strip()


# ----------------------------
# Q2 (operation=2)
# ----------------------------
def sql_q2_all_add(table: str, cols: Dict[str, str], partnumber: int) -> str:
    pn = cols["pn"]
    op = cols["op"]
    sl = cols["sl"]
    return """
    SELECT
      {sl} AS slotnumber,
      AVG(f1 + f3)        AS avg_f1_plus_f3,
      stddev_pop(f1 + f3) AS stddev_f1_plus_f3
    FROM {db}.{table}
    WHERE {pn} = {pnv} AND {op} = 2
    GROUP BY {sl}
    ORDER BY {sl}
    """.format(sl=sl, pn=pn, op=op, pnv=int(partnumber), db=DB_NAME, table=table).strip()


def sql_q2_all_add_rand(table: str, cols: Dict[str, str], partnumber: int, it: int) -> str:
    # Dummy predicate: changes SQL text without changing results
    pn = cols["pn"]
    op = cols["op"]
    sl = cols["sl"]
    return """
    SELECT
      {sl} AS slotnumber,
      AVG(f1 + f3)        AS avg_f1_plus_f3,
      stddev_pop(f1 + f3) AS stddev_f1_plus_f3
    FROM {db}.{table}
    WHERE {pn} = {pnv} AND {op} = 2
      AND ({pn} + 0*{sl}*{it}) = {pn}
    GROUP BY {sl}
    ORDER BY {sl}
    """.format(
        sl=sl, pn=pn, op=op, pnv=int(partnumber), it=int(it), db=DB_NAME, table=table
    ).strip()


def sql_q2_all_sub(table: str, cols: Dict[str, str], partnumber: int) -> str:
    pn = cols["pn"]
    op = cols["op"]
    sl = cols["sl"]
    return """
    SELECT
      {sl} AS slotnumber,
      AVG(f1 - f3)        AS avg_f1_minus_f3,
      stddev_pop(f1 - f3) AS stddev_f1_minus_f3
    FROM {db}.{table}
    WHERE {pn} = {pnv} AND {op} = 2
    GROUP BY {sl}
    ORDER BY {sl}
    """.format(sl=sl, pn=pn, op=op, pnv=int(partnumber), db=DB_NAME, table=table).strip()


def sql_q2_all_sub_rand(table: str, cols: Dict[str, str], partnumber: int, it: int) -> str:
    pn = cols["pn"]
    op = cols["op"]
    sl = cols["sl"]
    return """
    SELECT
      {sl} AS slotnumber,
      AVG(f1 - f3)        AS avg_f1_minus_f3,
      stddev_pop(f1 - f3) AS stddev_f1_minus_f3
    FROM {db}.{table}
    WHERE {pn} = {pnv} AND {op} = 2
      AND ({pn} + 0*{sl}*{it}) = {pn}
    GROUP BY {sl}
    ORDER BY {sl}
    """.format(
        sl=sl, pn=pn, op=op, pnv=int(partnumber), it=int(it), db=DB_NAME, table=table
    ).strip()


# ----------------------------
# Q3 (all operations)
# ----------------------------
def sql_q3_all_add(table: str, cols: Dict[str, str], partnumber: int) -> str:
    pn = cols["pn"]
    op = cols["op"]
    sl = cols["sl"]
    return """
    SELECT
      {sl} AS slotnumber,
      {op} AS operation,
      AVG(f1 + f3)        AS avg_f1_plus_f3,
      stddev_pop(f1 + f3) AS stddev_f1_plus_f3
    FROM {db}.{table}
    WHERE {pn} = {pnv}
    GROUP BY {sl}, {op}
    ORDER BY {sl}, {op}
    """.format(sl=sl, op=op, pn=pn, pnv=int(partnumber), db=DB_NAME, table=table).strip()


def sql_q3_all_add_rand(table: str, cols: Dict[str, str], partnumber: int, it: int) -> str:
    pn = cols["pn"]
    op = cols["op"]
    sl = cols["sl"]
    return """
    SELECT
      {sl} AS slotnumber,
      {op} AS operation,
      AVG(f1 + f3)        AS avg_f1_plus_f3,
      stddev_pop(f1 + f3) AS stddev_f1_plus_f3
    FROM {db}.{table}
    WHERE {pn} = {pnv}
      AND ({pn} + 0*{sl}*{it}) = {pn}
    GROUP BY {sl}, {op}
    ORDER BY {sl}, {op}
    """.format(
        sl=sl, op=op, pn=pn, pnv=int(partnumber), it=int(it), db=DB_NAME, table=table
    ).strip()


def sql_q3_all_sub(table: str, cols: Dict[str, str], partnumber: int) -> str:
    pn = cols["pn"]
    op = cols["op"]
    sl = cols["sl"]
    return """
    SELECT
      {sl} AS slotnumber,
      {op} AS operation,
      AVG(f1 - f3)        AS avg_f1_minus_f3,
      stddev_pop(f1 - f3) AS stddev_f1_minus_f3
    FROM {db}.{table}
    WHERE {pn} = {pnv}
    GROUP BY {sl}, {op}
    ORDER BY {sl}, {op}
    """.format(sl=sl, op=op, pn=pn, pnv=int(partnumber), db=DB_NAME, table=table).strip()


def sql_q3_all_sub_rand(table: str, cols: Dict[str, str], partnumber: int, it: int) -> str:
    pn = cols["pn"]
    op = cols["op"]
    sl = cols["sl"]
    return """
    SELECT
      {sl} AS slotnumber,
      {op} AS operation,
      AVG(f1 - f3)        AS avg_f1_minus_f3,
      stddev_pop(f1 - f3) AS stddev_f1_minus_f3
    FROM {db}.{table}
    WHERE {pn} = {pnv}
      AND ({pn} + 0*{sl}*{it}) = {pn}
    GROUP BY {sl}, {op}
    ORDER BY {sl}, {op}
    """.format(
        sl=sl, op=op, pn=pn, pnv=int(partnumber), it=int(it), db=DB_NAME, table=table
    ).strip()


# ----------------------------
# LAST6 (months filter across all years)
# ----------------------------
def sql_q2_last6_add(table: str, cols: Dict[str, str], partnumber: int) -> str:
    pn = cols["pn"]
    op = cols["op"]
    sl = cols["sl"]
    months = _months_list()
    return """
    SELECT
      {sl} AS slotnumber,
      AVG(f1 + f3)        AS avg_f1_plus_f3,
      stddev_pop(f1 + f3) AS stddev_f1_plus_f3
    FROM {db}.{table}
    WHERE month IN ({months})
      AND {pn} = {pnv} AND {op} = 2
    GROUP BY {sl}
    ORDER BY {sl}
    """.format(
        sl=sl, pn=pn, op=op, pnv=int(partnumber), months=months, db=DB_NAME, table=table
    ).strip()


def sql_q2_last6_add_rand(table: str, cols: Dict[str, str], partnumber: int, it: int) -> str:
    pn = cols["pn"]
    op = cols["op"]
    sl = cols["sl"]
    months = _months_list()
    return """
    SELECT
      {sl} AS slotnumber,
      AVG(f1 + f3)        AS avg_f1_plus_f3,
      stddev_pop(f1 + f3) AS stddev_f1_plus_f3
    FROM {db}.{table}
    WHERE month IN ({months})
      AND {pn} = {pnv} AND {op} = 2
      AND ({pn} + 0*{sl}*{it}) = {pn}
    GROUP BY {sl}
    ORDER BY {sl}
    """.format(
        sl=sl, pn=pn, op=op, pnv=int(partnumber), it=int(it), months=months, db=DB_NAME, table=table
    ).strip()


def sql_q2_last6_sub(table: str, cols: Dict[str, str], partnumber: int) -> str:
    pn = cols["pn"]
    op = cols["op"]
    sl = cols["sl"]
    months = _months_list()
    return """
    SELECT
      {sl} AS slotnumber,
      AVG(f1 - f3)        AS avg_f1_minus_f3,
      stddev_pop(f1 - f3) AS stddev_f1_minus_f3
    FROM {db}.{table}
    WHERE month IN ({months})
      AND {pn} = {pnv} AND {op} = 2
    GROUP BY {sl}
    ORDER BY {sl}
    """.format(
        sl=sl, pn=pn, op=op, pnv=int(partnumber), months=months, db=DB_NAME, table=table
    ).strip()


def sql_q2_last6_sub_rand(table: str, cols: Dict[str, str], partnumber: int, it: int) -> str:
    pn = cols["pn"]
    op = cols["op"]
    sl = cols["sl"]
    months = _months_list()
    return """
    SELECT
      {sl} AS slotnumber,
      AVG(f1 - f3)        AS avg_f1_minus_f3,
      stddev_pop(f1 - f3) AS stddev_f1_minus_f3
    FROM {db}.{table}
    WHERE month IN ({months})
      AND {pn} = {pnv} AND {op} = 2
      AND ({pn} + 0*{sl}*{it}) = {pn}
    GROUP BY {sl}
    ORDER BY {sl}
    """.format(
        sl=sl, pn=pn, op=op, pnv=int(partnumber), it=int(it), months=months, db=DB_NAME, table=table
    ).strip()


def sql_q3_last6_add(table: str, cols: Dict[str, str], partnumber: int) -> str:
    pn = cols["pn"]
    op = cols["op"]
    sl = cols["sl"]
    months = _months_list()
    return """
    SELECT
      {sl} AS slotnumber,
      {op} AS operation,
      AVG(f1 + f3)        AS avg_f1_plus_f3,
      stddev_pop(f1 + f3) AS stddev_f1_plus_f3
    FROM {db}.{table}
    WHERE month IN ({months})
      AND {pn} = {pnv}
    GROUP BY {sl}, {op}
    ORDER BY {sl}, {op}
    """.format(
        sl=sl, op=op, pn=pn, pnv=int(partnumber), months=months, db=DB_NAME, table=table
    ).strip()


def sql_q3_last6_add_rand(table: str, cols: Dict[str, str], partnumber: int, it: int) -> str:
    pn = cols["pn"]
    op = cols["op"]
    sl = cols["sl"]
    months = _months_list()
    return """
    SELECT
      {sl} AS slotnumber,
      {op} AS operation,
      AVG(f1 + f3)        AS avg_f1_plus_f3,
      stddev_pop(f1 + f3) AS stddev_f1_plus_f3
    FROM {db}.{table}
    WHERE month IN ({months})
      AND {pn} = {pnv}
      AND ({pn} + 0*{sl}*{it}) = {pn}
    GROUP BY {sl}, {op}
    ORDER BY {sl}, {op}
    """.format(
        sl=sl, op=op, pn=pn, pnv=int(partnumber), it=int(it), months=months, db=DB_NAME, table=table
    ).strip()


def sql_q3_last6_sub(table: str, cols: Dict[str, str], partnumber: int) -> str:
    pn = cols["pn"]
    op = cols["op"]
    sl = cols["sl"]
    months = _months_list()
    return """
    SELECT
      {sl} AS slotnumber,
      {op} AS operation,
      AVG(f1 - f3)        AS avg_f1_minus_f3,
      stddev_pop(f1 - f3) AS stddev_f1_minus_f3
    FROM {db}.{table}
    WHERE month IN ({months})
      AND {pn} = {pnv}
    GROUP BY {sl}, {op}
    ORDER BY {sl}, {op}
    """.format(
        sl=sl, op=op, pn=pn, pnv=int(partnumber), months=months, db=DB_NAME, table=table
    ).strip()


def sql_q3_last6_sub_rand(table: str, cols: Dict[str, str], partnumber: int, it: int) -> str:
    pn = cols["pn"]
    op = cols["op"]
    sl = cols["sl"]
    months = _months_list()
    return """
    SELECT
      {sl} AS slotnumber,
      {op} AS operation,
      AVG(f1 - f3)        AS avg_f1_minus_f3,
      stddev_pop(f1 - f3) AS stddev_f1_minus_f3
    FROM {db}.{table}
    WHERE month IN ({months})
      AND {pn} = {pnv}
      AND ({pn} + 0*{sl}*{it}) = {pn}
    GROUP BY {sl}, {op}
    ORDER BY {sl}, {op}
    """.format(
        sl=sl, op=op, pn=pn, pnv=int(partnumber), it=int(it), months=months, db=DB_NAME, table=table
    ).strip()


# ----------------------------
# Top PN getters (all years)
# ----------------------------
def get_top_partnumber_all_years_with_cursor(curs, table: str, cols: Dict[str, str]) -> Tuple[int, int]:
    _, res = hive_exec(curs, sql_get_top_partnumber_all_years(table, cols))
    if not res:
        raise ValueError("No rows found in table {}.{}".format(DB_NAME, table))
    return int(res[0][0]), int(res[0][1])


def get_top_partnumber_months_filter_with_cursor(
    curs, table: str, cols: Dict[str, str]
) -> Optional[Tuple[int, int]]:
    # If table has no data in these months (across all years), return None (do NOT raise).
    _, res = hive_exec(curs, sql_get_top_partnumber_months_filter(table, cols))
    if not res:
        return None
    return int(res[0][0]), int(res[0][1])
