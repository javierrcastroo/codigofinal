"""Enterprise database connectivity helpers for Spark, Hive, and Impala workloads.

This module centralizes session and cursor creation so data pipelines, table builders,
and benchmark scripts can share the same connection lifecycle and credentials logic.
"""

from pyspark.sql import SparkSession
import secrets
import random
import jaydebeapi
from impala.dbapi import connect
from impala.util import as_pandas


#   HANDLE HDFS WITH HIVE THROUGH JAYDEBEAPI
def hiveql_on():
  url_zk="jdbc:hive2://zk=dsip041dtlk.sercor.itp.es:2181/hiveserver2,dsip042dtlk.sercor.itp.es:2181/hiveserver2,dsip047dtlk.sercor.itp.es:2181/hiveserver2;AuthMech=1;KrbRealm=ITPDTLK.LAB;KrbHostFQDN=_HOST;KrbServiceName=hive;SSL=1;LogLevel=0;LogPath=/home/cdsw/Python/Hive/logs_conn;AllowSelfSignedCerts=1"
  conn_zk = jaydebeapi.connect("com.cloudera.hive.jdbc.HS2Driver",url_zk,['hive', 'hive'], "/opt/jars/HiveJDBC42.jar")
  curs = conn_zk.cursor()
  return curs, conn_zk 

curs, conn_zk =hiveql_on()

#   CLOSE CURSOR AND CONNECTION FROM JAYDEBEAPi
def hiveql_off(curs, conn_zk):
  curs.close()
  conn_zk.close()

hiveql_off(curs, conn_zk)

#   HANDLE HDFS WITH HIVE THROUGH PYSPARK 

def spark_on():
    token = secrets.token_hex(16)
    spark = (
        SparkSession.builder
        .appName(f"spark_session_{token}")
        .config("spark.eventLog.dir","hdfs:///user/spark/spark3ApplicationHistory")
        .config("spark.history.fs.logDirectory","hdfs:///user/spark/spark3ApplicationHistory")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
    spark.conf.set("spark.sql.hive.convertMetastoreParquet", "false")
    spark.conf.set("spark.atlas.hook.enabled", "false")

    return spark



IMPALA_HOSTS = [
    "dsip043dtlk.sercor.itp.es",
    "dsip044dtlk.sercor.itp.es",
    "dsip045dtlk.sercor.itp.es",
    "dsip046dtlk.sercor.itp.es",
]


def impala_on():
    """
    Devuelve (cursor, conn) a Impala usando el primer host disponible.
    Si falla un host, prueba el siguiente.
    """
    for h in IMPALA_HOSTS:
        try:
            conn = connect(
                host=h,
                port=21050,
                auth_mechanism="GSSAPI",
                kerberos_service_name="impala",
                use_ssl=True,
            )
            cursor = conn.cursor()
            return cursor, conn
        except Exception:
            pass

    raise RuntimeError("No hay Daemons disponibles")


def impala_off(cursor, conn):
    """
    Cierra cursor y conexión si están abiertos.
    """
    try:
        cursor.close()
    except Exception:
        pass

    try:
        conn.close()
    except Exception:
        pass
