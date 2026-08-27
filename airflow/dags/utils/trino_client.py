"""Small Trino query helper for the Airflow DAGs - same connection pattern
as chatbot/tools.py (trino.dbapi.connect, catalog=iceberg, schema=gold by
default). Duplicated rather than imported: chatbot/ and airflow/ are
separate Docker build contexts with no shared Python path.
"""

import os

import trino

TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))


def run_query(sql: str, catalog: str = "iceberg", schema: str = "gold") -> list[dict]:
    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="airflow",
        catalog=catalog,
        schema=schema,
    )
    try:
        cur = conn.cursor()
        cur.execute(sql)
        columns = [d[0] for d in cur.description]
        rows = cur.fetchall()
    finally:
        conn.close()
    return [dict(zip(columns, row)) for row in rows]
