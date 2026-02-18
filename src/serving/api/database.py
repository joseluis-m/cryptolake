"""
Conexión al Spark Thrift Server via PyHive.

Reutiliza la misma conexión que dbt usa (thrift en puerto 10000).
La API ejecuta queries SQL sobre las tablas Gold del star schema.
"""
import os

from pyhive import hive


# Dentro de Docker: spark-thrift. Desde Mac: localhost
THRIFT_HOST = os.getenv("THRIFT_HOST", "spark-thrift")
THRIFT_PORT = int(os.getenv("THRIFT_PORT", "10000"))


def get_connection():
    """Crea una conexión PyHive al Spark Thrift Server."""
    return hive.connect(
        host=THRIFT_HOST,
        port=THRIFT_PORT,
        auth="NOSASL",
    )


def execute_query(sql: str) -> list[dict]:
    """
    Ejecuta una query SQL y devuelve una lista de diccionarios.

    Ejemplo:
        rows = execute_query("SELECT * FROM cryptolake.gold.dim_coins")
        # [{"coin_id": "bitcoin", "avg_price": 95000.0, ...}, ...]
    """
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    finally:
        conn.close()
