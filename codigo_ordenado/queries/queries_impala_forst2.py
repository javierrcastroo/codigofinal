from __future__ import annotations

from time import perf_counter
from typing import Any, List, Optional, Tuple

from codigo_ordenado.utils.connection_to_db import impala_on, impala_off

DB_NAME = "sandbox_db"
LAST_6_MONTHS_2024 = (7, 8, 9, 10, 11, 12)


def impala_apply_session_settings(curs) -> None:
    for stmt in [
        "SET DISABLE_CACHED_READS=true",
    ]:
        try:
            curs.execute(stmt)
        except Exception:
            pass


def impala_exec(curs, sql: str) -> Tuple[float, List[Tuple[Any, ...]]]:
    t0 = perf_counter()
    curs.execute(sql)
    res = curs.fetchall()
    return perf_counter() - t0, res


def _months_list() -> str:
    return ", ".join(str(m) for m in LAST_6_MONTHS_2024)


# ----------------------------
# TOP partnumber
# ----------------------------
def sql_get_top_partnumber_2024(table: str) -> str:
    return f"""
    SELECT partnumber, COUNT(*) AS numrows
    FROM {DB_NAME}.{table}
    WHERE year = 2024
    GROUP BY partnumber
    ORDER BY numrows DESC
    LIMIT 1
    """.strip()


def sql_get_top_partnumber_2024_last6(table: str) -> str:
    months = _months_list()
    return f"""
    SELECT partnumber, COUNT(*) AS numrows
    FROM {DB_NAME}.{table}
    WHERE year = 2024 AND month IN ({months})
    GROUP BY partnumber
    ORDER BY numrows DESC
    LIMIT 1
    """.strip()


# ----------------------------
# Q1
# ----------------------------
def sql_q1_all(table: str) -> str:
    return f"""
    SELECT partnumber, operation, COUNT(*) AS numrows
    FROM {DB_NAME}.{table}
    WHERE year = 2024
    GROUP BY partnumber, operation
    ORDER BY numrows DESC
    """.strip()


def sql_q1_last6(table: str) -> str:
    months = _months_list()
    return f"""
    SELECT partnumber, operation, COUNT(*) AS numrows
    FROM {DB_NAME}.{table}
    WHERE year = 2024 AND month IN ({months})
    GROUP BY partnumber, operation
    ORDER BY numrows DESC
    """.strip()


# ----------------------------
# Q2 (operation=2)
# ----------------------------
def sql_q2_all_add(table: str, partnumber: int) -> str:
    return f"""
    SELECT
      slotnumber,
      AVG(f1 + f3)        AS avg_f1_plus_f3,
      STDDEV_POP(f1 + f3) AS stddev_f1_plus_f3
    FROM {DB_NAME}.{table}
    WHERE year = 2024 AND partnumber = {partnumber} AND operation = 2
    GROUP BY slotnumber
    ORDER BY slotnumber
    """.strip()


def sql_q2_all_add_rand(table: str, partnumber: int, it: int) -> str:
    return f"""
    SELECT
      slotnumber,
      AVG(f1 + f3)        AS avg_f1_plus_f3,
      STDDEV_POP(f1 + f3) AS stddev_f1_plus_f3
    FROM {DB_NAME}.{table}
    WHERE year = 2024 AND partnumber = {partnumber} AND operation = 2
      AND (partnumber + 0*slotnumber*{it}) = partnumber
    GROUP BY slotnumber
    ORDER BY slotnumber
    """.strip()


def sql_q2_all_sub(table: str, partnumber: int) -> str:
    return f"""
    SELECT
      slotnumber,
      AVG(f1 - f3)        AS avg_f1_minus_f3,
      STDDEV_POP(f1 - f3) AS stddev_f1_minus_f3
    FROM {DB_NAME}.{table}
    WHERE year = 2024 AND partnumber = {partnumber} AND operation = 2
    GROUP BY slotnumber
    ORDER BY slotnumber
    """.strip()


def sql_q2_all_sub_rand(table: str, partnumber: int, it: int) -> str:
    return f"""
    SELECT
      slotnumber,
      AVG(f1 - f3)        AS avg_f1_minus_f3,
      STDDEV_POP(f1 - f3) AS stddev_f1_minus_f3
    FROM {DB_NAME}.{table}
    WHERE year = 2024 AND partnumber = {partnumber} AND operation = 2
      AND (partnumber + 0*slotnumber*{it}) = partnumber
    GROUP BY slotnumber
    ORDER BY slotnumber
    """.strip()


# ----------------------------
# Q3 (all operations)
# ----------------------------
def sql_q3_all_add(table: str, partnumber: int) -> str:
    return f"""
    SELECT
      slotnumber,
      operation,
      AVG(f1 + f3)        AS avg_f1_plus_f3,
      STDDEV_POP(f1 + f3) AS stddev_f1_plus_f3
    FROM {DB_NAME}.{table}
    WHERE year = 2024 AND partnumber = {partnumber}
    GROUP BY slotnumber, operation
    ORDER BY slotnumber, operation
    """.strip()


def sql_q3_all_add_rand(table: str, partnumber: int, it: int) -> str:
    return f"""
    SELECT
      slotnumber,
      operation,
      AVG(f1 + f3)        AS avg_f1_plus_f3,
      STDDEV_POP(f1 + f3) AS stddev_f1_plus_f3
    FROM {DB_NAME}.{table}
    WHERE year = 2024 AND partnumber = {partnumber}
      AND (partnumber + 0*slotnumber*{it}) = partnumber
    GROUP BY slotnumber, operation
    ORDER BY slotnumber, operation
    """.strip()


def sql_q3_all_sub(table: str, partnumber: int) -> str:
    return f"""
    SELECT
      slotnumber,
      operation,
      AVG(f1 - f3)        AS avg_f1_minus_f3,
      STDDEV_POP(f1 - f3) AS stddev_f1_minus_f3
    FROM {DB_NAME}.{table}
    WHERE year = 2024 AND partnumber = {partnumber}
    GROUP BY slotnumber, operation
    ORDER BY slotnumber, operation
    """.strip()


def sql_q3_all_sub_rand(table: str, partnumber: int, it: int) -> str:
    return f"""
    SELECT
      slotnumber,
      operation,
      AVG(f1 - f3)        AS avg_f1_minus_f3,
      STDDEV_POP(f1 - f3) AS stddev_f1_minus_f3
    FROM {DB_NAME}.{table}
    WHERE year = 2024 AND partnumber = {partnumber}
      AND (partnumber + 0*slotnumber*{it}) = partnumber
    GROUP BY slotnumber, operation
    ORDER BY slotnumber, operation
    """.strip()


# ----------------------------
# LAST6
# ----------------------------
def sql_q2_last6_add(table: str, partnumber: int) -> str:
    months = _months_list()
    return f"""
    SELECT
      slotnumber,
      AVG(f1 + f3)        AS avg_f1_plus_f3,
      STDDEV_POP(f1 + f3) AS stddev_f1_plus_f3
    FROM {DB_NAME}.{table}
    WHERE year = 2024 AND month IN ({months})
      AND partnumber = {partnumber} AND operation = 2
    GROUP BY slotnumber
    ORDER BY slotnumber
    """.strip()


def sql_q2_last6_add_rand(table: str, partnumber: int, it: int) -> str:
    months = _months_list()
    return f"""
    SELECT
      slotnumber,
      AVG(f1 + f3)        AS avg_f1_plus_f3,
      STDDEV_POP(f1 + f3) AS stddev_f1_plus_f3
    FROM {DB_NAME}.{table}
    WHERE year = 2024 AND month IN ({months})
      AND partnumber = {partnumber} AND operation = 2
      AND (partnumber + 0*slotnumber*{it}) = partnumber
    GROUP BY slotnumber
    ORDER BY slotnumber
    """.strip()


def sql_q2_last6_sub(table: str, partnumber: int) -> str:
    months = _months_list()
    return f"""
    SELECT
      slotnumber,
      AVG(f1 - f3)        AS avg_f1_minus_f3,
      STDDEV_POP(f1 - f3) AS stddev_f1_minus_f3
    FROM {DB_NAME}.{table}
    WHERE year = 2024 AND month IN ({months})
      AND partnumber = {partnumber} AND operation = 2
    GROUP BY slotnumber
    ORDER BY slotnumber
    """.strip()


def sql_q2_last6_sub_rand(table: str, partnumber: int, it: int) -> str:
    months = _months_list()
    return f"""
    SELECT
      slotnumber,
      AVG(f1 - f3)        AS avg_f1_minus_f3,
      STDDEV_POP(f1 - f3) AS stddev_f1_minus_f3
    FROM {DB_NAME}.{table}
    WHERE year = 2024 AND month IN ({months})
      AND partnumber = {partnumber} AND operation = 2
      AND (partnumber + 0*slotnumber*{it}) = partnumber
    GROUP BY slotnumber
    ORDER BY slotnumber
    """.strip()


def sql_q3_last6_add(table: str, partnumber: int) -> str:
    months = _months_list()
    return f"""
    SELECT
      slotnumber,
      operation,
      AVG(f1 + f3)        AS avg_f1_plus_f3,
      STDDEV_POP(f1 + f3) AS stddev_f1_plus_f3
    FROM {DB_NAME}.{table}
    WHERE year = 2024 AND month IN ({months})
      AND partnumber = {partnumber}
    GROUP BY slotnumber, operation
    ORDER BY slotnumber, operation
    """.strip()


def sql_q3_last6_add_rand(table: str, partnumber: int, it: int) -> str:
    months = _months_list()
    return f"""
    SELECT
      slotnumber,
      operation,
      AVG(f1 + f3)        AS avg_f1_plus_f3,
      STDDEV_POP(f1 + f3) AS stddev_f1_plus_f3
    FROM {DB_NAME}.{table}
    WHERE year = 2024 AND month IN ({months})
      AND partnumber = {partnumber}
      AND (partnumber + 0*slotnumber*{it}) = partnumber
    GROUP BY slotnumber, operation
    ORDER BY slotnumber, operation
    """.strip()


def sql_q3_last6_sub(table: str, partnumber: int) -> str:
    months = _months_list()
    return f"""
    SELECT
      slotnumber,
      operation,
      AVG(f1 - f3)        AS avg_f1_minus_f3,
      STDDEV_POP(f1 - f3) AS stddev_f1_minus_f3
    FROM {DB_NAME}.{table}
    WHERE year = 2024 AND month IN ({months})
      AND partnumber = {partnumber}
    GROUP BY slotnumber, operation
    ORDER BY slotnumber, operation
    """.strip()


def sql_q3_last6_sub_rand(table: str, partnumber: int, it: int) -> str:
    months = _months_list()
    return f"""
    SELECT
      slotnumber,
      operation,
      AVG(f1 - f3)        AS avg_f1_minus_f3,
      STDDEV_POP(f1 - f3) AS stddev_f1_minus_f3
    FROM {DB_NAME}.{table}
    WHERE year = 2024 AND month IN ({months})
      AND partnumber = {partnumber}
      AND (partnumber + 0*slotnumber*{it}) = partnumber
    GROUP BY slotnumber, operation
    ORDER BY slotnumber, operation
    """.strip()


# ----------------------------
# Top PN getters (IMPORTANT CHANGE)
# ----------------------------
def get_top_partnumber_2024_with_cursor(curs, table: str) -> Tuple[int, int]:
    _, res = impala_exec(curs, sql_get_top_partnumber_2024(table))
    if not res:
        raise ValueError("No rows found for year 2024 in table {}.{}".format(DB_NAME, table))
    return int(res[0][0]), int(res[0][1])


def get_top_partnumber_2024_last6_with_cursor(curs, table: str) -> Optional[Tuple[int, int]]:
    # If table has no data in last6 months, return None (do NOT raise).
    _, res = impala_exec(curs, sql_get_top_partnumber_2024_last6(table))
    if not res:
        return None
    return int(res[0][0]), int(res[0][1])


# Optional legacy wrapper
def get_top_partnumber_2024(table: str) -> Tuple[int, int]:
    curs, conn = impala_on()
    impala_apply_session_settings(curs)
    out = get_top_partnumber_2024_with_cursor(curs, table)
    impala_off(curs, conn)
    return out
