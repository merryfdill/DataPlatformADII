"""Bronze ingestion: load the two MVP sources (BADR SQLite, scraping CSV) as
Parquet objects into MinIO's `datalake` bucket, with only a technical
SQLite/CSV -> Parquet conversion. No business cleaning, no matching, no
derived columns - that belongs to Silver (Spark), not here.

Reuses the MinIO bucket and credentials already provisioned in Phase 1
(docker-compose.yml / .env) - this script does not create a bucket or
touch any infrastructure file.

Note on endpoints: this script runs on the HOST (not inside a container),
so it reaches MinIO via the published host port
(config.MINIO_ENDPOINT_HOST, default http://localhost:9000) rather than
the Docker-internal hostname (MINIO_ENDPOINT=http://minio:9000) that
containers on the compose network use.

Idempotence: each run re-uploads to the same two fixed object keys
(bronze/badr/badr.parquet, bronze/scraping/prix_web.parquet), so a rerun
overwrites cleanly instead of accumulating files.

Usage
-----
    python ingestion/bronze_ingestion.py
"""

import io
import logging
import sqlite3
import sys
from pathlib import Path

import boto3
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("bronze_ingestion")


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=config.MINIO_ENDPOINT_HOST,
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
        region_name=config.AWS_REGION,
    )


def dataframe_to_parquet_bytes(df):
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False)
    return buf.getvalue()


def upload_bytes(s3, data, bucket, key):
    s3.put_object(Bucket=bucket, Key=key, Body=data)
    head = s3.head_object(Bucket=bucket, Key=key)
    return head["ContentLength"]


def ingest_badr(s3):
    if not config.BADR_DB_PATH.exists():
        raise FileNotFoundError(f"BADR source not found: {config.BADR_DB_PATH}")

    conn = sqlite3.connect(config.BADR_DB_PATH)
    try:
        df = pd.read_sql_query(f"SELECT * FROM {config.BADR_TABLE_NAME}", conn)
    finally:
        conn.close()

    logger.info("BADR: read %d rows / %d columns from %s", len(df), df.shape[1], config.BADR_DB_PATH)

    parquet_bytes = dataframe_to_parquet_bytes(df)
    size = upload_bytes(s3, parquet_bytes, config.MINIO_BUCKET, config.BRONZE_BADR_KEY)
    logger.info("BADR: uploaded %d bytes to s3://%s/%s", size, config.MINIO_BUCKET, config.BRONZE_BADR_KEY)
    return df


def ingest_scraping(s3):
    if not config.SCRAPING_OUTPUT_CSV.exists():
        raise FileNotFoundError(f"Scraping source not found: {config.SCRAPING_OUTPUT_CSV}")

    df = pd.read_csv(config.SCRAPING_OUTPUT_CSV)
    logger.info(
        "Scraping: read %d rows / %d columns from %s", len(df), df.shape[1], config.SCRAPING_OUTPUT_CSV
    )

    parquet_bytes = dataframe_to_parquet_bytes(df)
    size = upload_bytes(s3, parquet_bytes, config.MINIO_BUCKET, config.BRONZE_SCRAPING_KEY)
    logger.info(
        "Scraping: uploaded %d bytes to s3://%s/%s", size, config.MINIO_BUCKET, config.BRONZE_SCRAPING_KEY
    )
    return df


def verify_roundtrip(s3, bucket, key, local_df, label):
    obj = s3.get_object(Bucket=bucket, Key=key)
    remote_df = pd.read_parquet(io.BytesIO(obj["Body"].read()), engine="pyarrow")

    ok_rows = len(remote_df) == len(local_df)
    ok_cols = list(remote_df.columns) == list(local_df.columns)
    status = "OK" if (ok_rows and ok_cols) else "MISMATCH"
    logger.info(
        "%s: local rows=%d, bronze rows=%d, columns match=%s -> %s",
        label,
        len(local_df),
        len(remote_df),
        ok_cols,
        status,
    )
    if not (ok_rows and ok_cols):
        raise ValueError(f"{label}: bronze round-trip does not match the source (rows/columns mismatch).")
    return remote_df


def main():
    s3 = get_s3_client()

    try:
        s3.head_bucket(Bucket=config.MINIO_BUCKET)
    except Exception as exc:
        raise RuntimeError(
            f"Bucket '{config.MINIO_BUCKET}' not reachable at {config.MINIO_ENDPOINT_HOST} "
            f"- is the Phase 1 MinIO stack running? ({exc})"
        ) from exc

    badr_df = ingest_badr(s3)
    scraping_df = ingest_scraping(s3)

    verify_roundtrip(s3, config.MINIO_BUCKET, config.BRONZE_BADR_KEY, badr_df, "BADR")
    verify_roundtrip(s3, config.MINIO_BUCKET, config.BRONZE_SCRAPING_KEY, scraping_df, "Scraping")

    logger.info("Bronze ingestion complete.")


if __name__ == "__main__":
    main()
