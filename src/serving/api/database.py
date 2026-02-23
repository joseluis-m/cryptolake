"""
Spark Thrift Server connection via PyHive.

Reuses the same Thrift connection that dbt uses (port 10000).
The API executes SQL queries against the Gold star schema tables.
"""

import os

from pyhive import hive

THRIFT_HOST = os.getenv("THRIFT_HOST", "spark-thrift")
THRIFT_PORT = int(os.getenv("THRIFT_PORT", "10000"))


def get_connection():
    """Create a PyHive connection to the Spark Thrift Server."""
    return hive.connect(
        host=THRIFT_HOST,
        port=THRIFT_PORT,
        auth="NOSASL",
    )


def execute_query(sql: str) -> list[dict]:
    """Execute a SQL query and return a list of dictionaries."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    finally:
        conn.close()
