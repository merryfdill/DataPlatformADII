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

Idempotence, legacy mode (--run-date omitted): each run re-uploads to the
same two fixed object keys (bronze/badr/badr.parquet,
bronze/scraping/prix_web.parquet), so a rerun overwrites cleanly instead of
accumulating files. Still what the currently-live main_pipeline.py relies on.

Partitioned mode (--run-date given, 'YYYY-MM-DD'): writes to
bronze/badr/date=.../badr.parquet and bronze/scraping/date=.../prix_web.parquet
instead - one partition per day, never overwritten by a later run with a
different date (see ingestion/config.py bronze_badr_key/bronze_scraping_key
for exactly what each partition holds). Added for the daily-simulation
phase (Etape 2) - not yet wired into any live DAG.

Usage
-----
    python ingestion/bronze_ingestion.py                        # legacy fixed keys
    python ingestion/bronze_ingestion.py --run-date 2026-09-01   # partitioned
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


def ingest_badr(s3, run_date=None):
    """run_date (optional, 'YYYY-MM-DD'): when given, writes to the
    partitioned bronze/badr/date=.../ layout (config.bronze_badr_key)
    instead of the legacy fixed key - additive, not a breaking change: the
    currently-live main_pipeline.py calls this with no run_date and keeps
    getting the exact same fixed-key behavior as before.
    """
    if not config.BADR_DB_PATH.exists():
        raise FileNotFoundError(f"BADR source not found: {config.BADR_DB_PATH}")

    conn = sqlite3.connect(config.BADR_DB_PATH)
    try:
        df = pd.read_sql_query(f"SELECT * FROM {config.BADR_TABLE_NAME}", conn)
    finally:
        conn.close()

    logger.info("BADR: read %d rows / %d columns from %s", len(df), df.shape[1], config.BADR_DB_PATH)

    key = config.bronze_badr_key(run_date) if run_date else config.BRONZE_BADR_KEY
    parquet_bytes = dataframe_to_parquet_bytes(df)
    size = upload_bytes(s3, parquet_bytes, config.MINIO_BUCKET, key)
    logger.info("BADR: uploaded %d bytes to s3://%s/%s", size, config.MINIO_BUCKET, key)
    return df


def ingest_scraping(s3, run_date=None):
    """run_date (optional, 'YYYY-MM-DD'): when given, writes to the
    partitioned bronze/scraping/date=.../ layout (config.bronze_scraping_key)
    instead of the legacy fixed key - same additive/non-breaking contract
    as ingest_badr above.
    """
    if not config.SCRAPING_OUTPUT_CSV.exists():
        raise FileNotFoundError(f"Scraping source not found: {config.SCRAPING_OUTPUT_CSV}")

    df = pd.read_csv(config.SCRAPING_OUTPUT_CSV)
    logger.info(
        "Scraping: read %d rows / %d columns from %s", len(df), df.shape[1], config.SCRAPING_OUTPUT_CSV
    )

    key = config.bronze_scraping_key(run_date) if run_date else config.BRONZE_SCRAPING_KEY
    parquet_bytes = dataframe_to_parquet_bytes(df)
    size = upload_bytes(s3, parquet_bytes, config.MINIO_BUCKET, key)
    logger.info("Scraping: uploaded %d bytes to s3://%s/%s", size, config.MINIO_BUCKET, key)
    return df


def count_badr_declarations(date_debut=None, date_fin=None):
    """Direct SQLite count against data/badr.db - the authoritative current
    BADR population (grows via generate_badr.py --run-date appends, Etape 1).
    Replaces what used to be a hardcoded 5000 in main_pipeline.py's
    data_quality_extra/validation_finale/collect_metrics - those numbers
    become wrong the moment BADR grows past its original 5000 rows.

    date_debut/date_fin (optional, 'YYYY-MM-DD', both or neither): when
    given, counts only declarations within [date_debut, date_fin]
    inclusive - the actual population being arbitrated in one arbitrage.py
    run, not the whole table.
    """
    if not config.BADR_DB_PATH.exists():
        raise FileNotFoundError(f"BADR source not found: {config.BADR_DB_PATH}")
    conn = sqlite3.connect(config.BADR_DB_PATH)
    try:
        if date_debut and date_fin:
            cur = conn.execute(
                f"SELECT COUNT(*) FROM {config.BADR_TABLE_NAME} WHERE DATE_DEPOT BETWEEN ? AND ?",
                (date_debut, date_fin),
            )
        else:
            cur = conn.execute(f"SELECT COUNT(*) FROM {config.BADR_TABLE_NAME}")
        return cur.fetchone()[0]
    finally:
        conn.close()


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
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--run-date",
        type=str,
        default=None,
        help=(
            "YYYY-MM-DD - writes to the partitioned bronze/*/date=.../ layout "
            "instead of the legacy fixed keys. Omit for the original behavior "
            "(still what the currently-live main_pipeline.py relies on)."
        ),
    )
    args = parser.parse_args()

    s3 = get_s3_client()

    try:
        s3.head_bucket(Bucket=config.MINIO_BUCKET)
    except Exception as exc:
        raise RuntimeError(
            f"Bucket '{config.MINIO_BUCKET}' not reachable at {config.MINIO_ENDPOINT_HOST} "
            f"- is the Phase 1 MinIO stack running? ({exc})"
        ) from exc

    badr_df = ingest_badr(s3, args.run_date)
    scraping_df = ingest_scraping(s3, args.run_date)

    badr_key = config.bronze_badr_key(args.run_date) if args.run_date else config.BRONZE_BADR_KEY
    scraping_key = config.bronze_scraping_key(args.run_date) if args.run_date else config.BRONZE_SCRAPING_KEY
    verify_roundtrip(s3, config.MINIO_BUCKET, badr_key, badr_df, "BADR")
    verify_roundtrip(s3, config.MINIO_BUCKET, scraping_key, scraping_df, "Scraping")

    logger.info("Bronze ingestion complete.")


if __name__ == "__main__":
    main()
