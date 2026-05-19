"""Declarative table provisioning for Hive/Impala analytical environments.

The module consumes JSON specifications and creates database objects required by
preprocessing outputs, synthetic generators, and query benchmarking scripts.
"""

from __future__ import annotations

import argparse
from typing import Dict, List, Tuple

from codigo_ordenado.utils.json_config import load_json
from codigo_ordenado.utils.connection_to_db import hiveql_on, hiveql_off, impala_on, impala_off


def build_table_name(partitioning: str, block_mb: int, engine: str) -> str:
    if partitioning == "YYYY_MM":
        part = "YYYY_MM"
    elif partitioning == "YYYY_MM_PN":
        part = "YYYY_MM_PN"
    else:
        part = "YYYY_MM_PN_OP"
    return "compacted_partitioned_{0}_{1}MB_{2}".format(part, int(block_mb), engine)


def partition_cols_for(partitioning: str) -> List[Tuple[str, str]]:
    if partitioning == "YYYY_MM":
        return [("year", "INT"), ("month", "INT")]
    if partitioning == "YYYY_MM_PN":
        return [("year", "INT"), ("month", "INT"), ("partnumber", "BIGINT")]
    return [("year", "INT"), ("month", "INT"), ("partnumber", "BIGINT"), ("operation", "BIGINT")]


def cols_sql(cols: List[Tuple[str, str]]) -> str:
    lines = []
    for name, typ in cols:
        lines.append("  {0} {1}".format(name, typ))
    return ",\n".join(lines)


def data_cols_for_partitioning(schema_cols: List[Tuple[str, str]], partitioning: str) -> List[Tuple[str, str]]:
    part_cols = partition_cols_for(partitioning)
    part_names = set([c[0] for c in part_cols])

    cols: List[Tuple[str, str]] = []
    for name, typ in schema_cols:
        if name in part_names:
            continue
        cols.append((name, typ))
    return cols


def create_external_table_sql(
    db: str,
    table: str,
    location: str,
    partitioning: str,
    schema_cols: List[Tuple[str, str]],
) -> List[str]:
    data_cols = data_cols_for_partitioning(schema_cols, partitioning)
    part_cols = partition_cols_for(partitioning)

    drop_sql = "DROP TABLE IF EXISTS {0}.{1}".format(db, table)

    create_sql = (
        "CREATE EXTERNAL TABLE {0}.{1} (\n"
        "{2}\n"
        ")\n"
        "PARTITIONED BY (\n"
        "{3}\n"
        ")\n"
        "STORED AS PARQUET\n"
        "LOCATION '{4}'"
    ).format(
        db,
        table,
        cols_sql(data_cols),
        cols_sql(part_cols),
        location,
    )

    return [drop_sql, create_sql]


def hive_msck_sql(db: str, table: str) -> str:
    return "MSCK REPAIR TABLE {0}.{1}".format(db, table)


def impala_refresh_sql(db: str, table: str) -> List[str]:
    return [
        "INVALIDATE METADATA {0}.{1}".format(db, table),
        "REFRESH {0}.{1}".format(db, table),
    ]


def run_hive_tables(db: str, specs: List[Dict[str, object]]) -> None:
    curs, conn = hiveql_on()

    for spec in specs:
        table = spec["table"]          # type: ignore[assignment]
        location = spec["location"]    # type: ignore[assignment]
        partitioning = spec["partitioning"]  # type: ignore[assignment]
        schema_cols = spec["schema_cols"]    # type: ignore[assignment]

        stmts = create_external_table_sql(db, table, location, partitioning, schema_cols)
        stmts.append(hive_msck_sql(db, table))

        for s in stmts:
            curs.execute(s)

        print("[OK] hive: {0}.{1} -> {2}".format(db, table, location))

    hiveql_off(curs, conn)


def run_impala_tables_with_hive_repair(db: str, specs: List[Dict[str, object]]) -> None:
    """
    Create tables in Impala, but load partitions via Hive MSCK (shared metastore),
    then refresh in Impala.
    """
    icurs, iconn = impala_on()
    hcurs, hconn = hiveql_on()

    for spec in specs:
        table = spec["table"]          # type: ignore[assignment]
        location = spec["location"]    # type: ignore[assignment]
        partitioning = spec["partitioning"]  # type: ignore[assignment]
        schema_cols = spec["schema_cols"]    # type: ignore[assignment]

        # 1) Create in Impala
        stmts = create_external_table_sql(db, table, location, partitioning, schema_cols)
        for s in stmts:
            icurs.execute(s)

        # 2) Load partitions into the metastore (Hive side)
        hcurs.execute(hive_msck_sql(db, table))

        # 3) Refresh in Impala
        for s in impala_refresh_sql(db, table):
            icurs.execute(s)

        print("[OK] impala: {0}.{1} -> {2} (MSCK + REFRESH)".format(db, table, location))

    hiveql_off(hcurs, hconn)
    impala_off(icurs, iconn)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="codigo_ordenado/config/create_tables.json")
    args = parser.parse_args()

    cfg = load_json(args.config)

    db = cfg["db_name"]

    pre_hive_root = cfg["preprocessed_hive_root"].rstrip("/")
    pre_imp_root = cfg["preprocessed_impala_root"].rstrip("/")

    forst_hive_root = cfg["forst2hf_hive_root"].rstrip("/")
    forst_imp_root = cfg["forst2hf_impala_root"].rstrip("/")

    forst_hive_table = cfg["forst2hf_hive_table"]
    forst_imp_table = cfg["forst2hf_impala_table"]

    # PREPROCESSED schema
    pre_schema_cols: List[Tuple[str, str]] = [
        ("eventtimestamp", "DOUBLE"),
        ("f1", "DOUBLE"),
        ("f2", "DOUBLE"),
        ("f3", "DOUBLE"),
        ("f4", "DOUBLE"),
        ("ordernumber", "BIGINT"),
        ("partnumber", "BIGINT"),
        ("operation", "BIGINT"),
        ("slotnumber", "BIGINT"),
    ]

    # FORST2HF typed schema (as you said)
    forst_schema_cols: List[Tuple[str, str]] = [
        ("eventtimestamp", "BIGINT"),
        ("f1", "DOUBLE"),
        ("f2", "DOUBLE"),
        ("f3", "DOUBLE"),
        ("f4", "DOUBLE"),
        ("ordernumber", "STRING"),
        ("partnumber", "BIGINT"),
        ("operation", "BIGINT"),
        ("slotnumber", "INT"),
    ]

    layouts = [
        ("YYYY_MM", 64),
        ("YYYY_MM_PN", 64),
        ("YYYY_MM_PN_OP", 64),
        ("YYYY_MM", 128),
        ("YYYY_MM_PN", 128),
        ("YYYY_MM", 256),
        ("YYYY_MM_PN", 256),
    ]

    specs_hive: List[Dict[str, object]] = []
    specs_impala: List[Dict[str, object]] = []

    # 14 preprocessed tables
    for partitioning, block_mb in layouts:
        t_hive = build_table_name(partitioning, block_mb, "hive")
        t_imp = build_table_name(partitioning, block_mb, "impala")

        loc_hive = "{0}/{1}".format(pre_hive_root, t_hive)
        loc_imp = "{0}/{1}".format(pre_imp_root, t_imp)

        specs_hive.append({"table": t_hive, "location": loc_hive, "partitioning": partitioning, "schema_cols": pre_schema_cols})
        specs_impala.append({"table": t_imp, "location": loc_imp, "partitioning": partitioning, "schema_cols": pre_schema_cols})

    # 2 FORST2HF typed tables (fixed locations, YYYY_MM_PN_OP)
    forst_partitioning = "YYYY_MM_PN_OP"
    specs_hive.append({"table": forst_hive_table, "location": forst_hive_root, "partitioning": forst_partitioning, "schema_cols": forst_schema_cols})
    specs_impala.append({"table": forst_imp_table, "location": forst_imp_root, "partitioning": forst_partitioning, "schema_cols": forst_schema_cols})

    # Create Hive tables normally (CREATE + MSCK)
    run_hive_tables(db, specs_hive)

    # Create Impala tables + load partitions via Hive MSCK + refresh in Impala
    run_impala_tables_with_hive_repair(db, specs_impala)


if __name__ == "__main__":
    main()
