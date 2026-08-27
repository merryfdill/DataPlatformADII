"""Reads/writes the reporting.* tables in the SAME Postgres database Airflow
already uses for its own metadata (new schema `reporting`, not a new
database service).

Connection: Airflow 3 deliberately scrubs AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
to "airflow-db-not-allowed:///" inside task subprocesses, so this module can no
longer derive the DSN from it (that gave "ProgrammingError: invalid dsn" and
silently recorded zero metrics). It builds an explicit connection from
POSTGRES_USER / POSTGRES_PASSWORD / POSTGRES_DB (injected into every Airflow
container via docker-compose's x-airflow-common) against host "postgres".
"""

import os
from datetime import datetime

import psycopg2
import psycopg2.extras

from utils.config import REPORTING_SCHEMA


def _get_connection():
    return psycopg2.connect(
        host=os.environ.get("REPORTING_DB_HOST", "postgres"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


DDL = f"""
CREATE SCHEMA IF NOT EXISTS {REPORTING_SCHEMA};

CREATE TABLE IF NOT EXISTS {REPORTING_SCHEMA}.pipeline_runs (
    run_id VARCHAR PRIMARY KEY,
    logical_date TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    global_status VARCHAR,
    notes TEXT,
    dag_id VARCHAR
);

-- Etape 3 migration: dag_id did not exist while there was only ever one DAG
-- (adii_main_pipeline) writing here. Now that adii_daily_ingestion and
-- adii_arbitrage both write rows to this same table, report_generator.py
-- needs to know which DAG produced a given run_id to build a working
-- Airflow URL (dags/<dag_id>/grid?dag_run_id=...) - a hardcoded dag_id
-- pointed at a retired DAG, a dead link. ADD COLUMN IF NOT EXISTS is
-- idempotent - safe to run on every start_run() call, on a fresh table or
-- an existing one from before this migration.
ALTER TABLE {REPORTING_SCHEMA}.pipeline_runs ADD COLUMN IF NOT EXISTS dag_id VARCHAR;

CREATE TABLE IF NOT EXISTS {REPORTING_SCHEMA}.task_runs (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR NOT NULL REFERENCES {REPORTING_SCHEMA}.pipeline_runs(run_id),
    task_id VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,
    duration_seconds DOUBLE PRECISION,
    retries INTEGER DEFAULT 0,
    error_category VARCHAR,
    error_message TEXT,
    UNIQUE (run_id, task_id)
);

CREATE TABLE IF NOT EXISTS {REPORTING_SCHEMA}.business_metrics (
    run_id VARCHAR PRIMARY KEY REFERENCES {REPORTING_SCHEMA}.pipeline_runs(run_id),
    logical_date TIMESTAMPTZ NOT NULL,
    nb_declarations INTEGER,
    nb_normal INTEGER,
    nb_minore INTEGER,
    nb_majore INTEGER,
    pct_normal DOUBLE PRECISION,
    pct_minore DOUBLE PRECISION,
    pct_majore DOUBLE PRECISION,
    ratio_moyen DOUBLE PRECISION,
    ratio_median DOUBLE PRECISION,
    ratio_min DOUBLE PRECISION,
    ratio_max DOUBLE PRECISION,
    valeur_declaree_moyenne DOUBLE PRECISION,
    prix_reference_moyen DOUBLE PRECISION,
    valeur_totale_mad DOUBLE PRECISION,
    quantite_totale BIGINT,
    nb_hors_perimetre INTEGER,
    nb_sans_code_ngp INTEGER
);

CREATE TABLE IF NOT EXISTS {REPORTING_SCHEMA}.data_quality_runs (
    run_id VARCHAR PRIMARY KEY REFERENCES {REPORTING_SCHEMA}.pipeline_runs(run_id),
    dbt_tests_passed INTEGER,
    dbt_tests_failed INTEGER,
    duplicates INTEGER,
    nulls_critical INTEGER,
    invalid_code_ngp INTEGER,
    invalid_prices INTEGER,
    invalid_ratios INTEGER,
    unmatched_products INTEGER,
    rows_out_of_scope INTEGER
);

CREATE TABLE IF NOT EXISTS {REPORTING_SCHEMA}.scraping_counts (
    run_id VARCHAR NOT NULL REFERENCES {REPORTING_SCHEMA}.pipeline_runs(run_id),
    categorie VARCHAR NOT NULL,
    nb_produits INTEGER NOT NULL,
    UNIQUE (run_id, categorie)
);
"""


def ensure_schema() -> None:
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()
    finally:
        conn.close()


def start_run(run_id: str, logical_date: datetime, dag_id: str | None = None) -> None:
    """dag_id (optional, additive): identifies which DAG this run_id
    belongs to (adii_daily_ingestion, adii_arbitrage, ...) - needed since
    Etape 3 split the single main_pipeline DAG in two, both writing rows
    here. Omitted, stays NULL (harmless - only affects the Slack report's
    "Voir les details" link, which falls back to a sane default).
    """
    ensure_schema()
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {REPORTING_SCHEMA}.pipeline_runs (run_id, logical_date, started_at, global_status, dag_id)
                VALUES (%s, %s, now(), 'RUNNING', %s)
                ON CONFLICT (run_id) DO UPDATE SET started_at = now(), global_status = 'RUNNING', dag_id = EXCLUDED.dag_id
                """,
                (run_id, logical_date, dag_id),
            )
        conn.commit()
    finally:
        conn.close()


def finish_run(run_id: str, global_status: str, notes: str = "") -> None:
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {REPORTING_SCHEMA}.pipeline_runs
                SET finished_at = now(), global_status = %s, notes = %s
                WHERE run_id = %s
                """,
                (global_status, notes, run_id),
            )
        conn.commit()
    finally:
        conn.close()


def record_task_run(
    run_id: str,
    task_id: str,
    status: str,
    start_time: datetime,
    end_time: datetime,
    retries: int = 0,
    error_category: str | None = None,
    error_message: str | None = None,
) -> None:
    duration = (end_time - start_time).total_seconds() if start_time and end_time else None
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {REPORTING_SCHEMA}.task_runs
                    (run_id, task_id, status, start_time, end_time, duration_seconds, retries, error_category, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, task_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    end_time = EXCLUDED.end_time,
                    duration_seconds = EXCLUDED.duration_seconds,
                    retries = EXCLUDED.retries,
                    error_category = EXCLUDED.error_category,
                    error_message = EXCLUDED.error_message
                """,
                (run_id, task_id, status, start_time, end_time, duration, retries, error_category, error_message),
            )
        conn.commit()
    finally:
        conn.close()


def record_business_metrics(run_id: str, logical_date: datetime, metrics: dict) -> None:
    conn = _get_connection()
    try:
        cols = ["run_id", "logical_date"] + list(metrics.keys())
        placeholders = ", ".join(["%s"] * len(cols))
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in metrics.keys())
        values = [run_id, logical_date] + list(metrics.values())
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {REPORTING_SCHEMA}.business_metrics ({", ".join(cols)})
                VALUES ({placeholders})
                ON CONFLICT (run_id) DO UPDATE SET {updates}
                """,
                values,
            )
        conn.commit()
    finally:
        conn.close()


def record_data_quality(run_id: str, dq: dict) -> None:
    conn = _get_connection()
    try:
        cols = ["run_id"] + list(dq.keys())
        placeholders = ", ".join(["%s"] * len(cols))
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in dq.keys())
        values = [run_id] + list(dq.values())
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {REPORTING_SCHEMA}.data_quality_runs ({", ".join(cols)})
                VALUES ({placeholders})
                ON CONFLICT (run_id) DO UPDATE SET {updates}
                """,
                values,
            )
        conn.commit()
    finally:
        conn.close()


def record_scraping_counts(run_id: str, counts: dict) -> None:
    """counts: {categorie: nb_produits}, e.g. from data/prix_web.csv's own
    value_counts() after a scrape_web run - lets the 07:00 report show the
    real per-category volume and its day-to-day variation (see
    docs/airflow_pipeline.md - scraping against a live third-party site has
    shown swings from 21 to 70 total products across consecutive runs).
    """
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            for categorie, nb_produits in counts.items():
                cur.execute(
                    f"""
                    INSERT INTO {REPORTING_SCHEMA}.scraping_counts (run_id, categorie, nb_produits)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (run_id, categorie) DO UPDATE SET nb_produits = EXCLUDED.nb_produits
                    """,
                    (run_id, categorie, nb_produits),
                )
        conn.commit()
    finally:
        conn.close()


def get_latest_run() -> dict | None:
    conn = _get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"SELECT * FROM {REPORTING_SCHEMA}.pipeline_runs ORDER BY started_at DESC NULLS LAST LIMIT 1"
            )
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()


def get_run_details(run_id: str) -> dict:
    conn = _get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"SELECT * FROM {REPORTING_SCHEMA}.task_runs WHERE run_id = %s ORDER BY start_time", (run_id,)
            )
            task_runs = [dict(r) for r in cur.fetchall()]
            cur.execute(f"SELECT * FROM {REPORTING_SCHEMA}.business_metrics WHERE run_id = %s", (run_id,))
            biz = cur.fetchone()
            cur.execute(f"SELECT * FROM {REPORTING_SCHEMA}.data_quality_runs WHERE run_id = %s", (run_id,))
            dq = cur.fetchone()
    finally:
        conn.close()
    return {
        "task_runs": task_runs,
        "business_metrics": dict(biz) if biz else None,
        "data_quality": dict(dq) if dq else None,
    }


def get_task_states(dag_id: str, run_id: str) -> dict:
    """Reads task states directly from Airflow's OWN metadata table
    (task_instance), not the reporting schema. Needed because Airflow 3.x's
    TaskFlow SDK context exposes a leaner DagRun object
    (airflow.sdk.DagRun) that has no get_task_instances() method - unlike
    the classic 2.x ORM DagRun model. Same connection as every other
    function here (explicit POSTGRES_* creds, see _get_connection), just a
    different table that already exists in the same database (Airflow
    itself writes task_instance), no new schema needed.
    """
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT task_id, state FROM task_instance WHERE dag_id = %s AND run_id = %s",
                (dag_id, run_id),
            )
            return dict(cur.fetchall())
    finally:
        conn.close()


def get_trend(days: int = 7) -> list[dict]:
    conn = _get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT bm.*, pr.global_status
                FROM {REPORTING_SCHEMA}.business_metrics bm
                JOIN {REPORTING_SCHEMA}.pipeline_runs pr ON pr.run_id = bm.run_id
                ORDER BY bm.logical_date DESC
                LIMIT %s
                """,
                (days,),
            )
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()
