"""ADII daily ingestion - Etape 3 (daily-simulation phase): the first half
of the former adii_main_pipeline (see main_pipeline.py.bak), redistributed
into its own DAG stopping right after valeur_mad_quantite.

Why split here: prix_reference/ratio_unitaire/arbitrage are BUSINESS
JUDGMENTS over a period of declarations (percentile thresholds computed
over the population being judged). Recomputing them every night on a
growing BADR population would silently reclassify old, already-judged
declarations (observed: a reference price moving 1549->1699 MAD between
two runs shifts the P10/P90 cut points for everyone, not just new rows).
Collecting new declarations daily and judging them are two different
operations with two different cadences - this DAG only does the former.
BADR is no longer frozen (Etape 1 reverses that earlier decision,
deliberately - see README/plan) but the judgment itself must still be.

Schedule: 03:00 Africa/Casablanca daily. Reuses the same utils/ package as
arbitrage.py and the retired main_pipeline.py - nothing in utils/ changed
for this split.
"""

import logging
import subprocess
import sys

import pendulum

from airflow.sdk import DAG, task

sys.path.insert(0, "/opt/airflow/ingestion")
sys.path.insert(0, "/opt/airflow/ingestion/ml")

from utils import metrics
from utils.docker_exec import run_spark_job
from utils.error_handler import (
    RETRY_POLICY_DATA,
    RETRY_POLICY_INFRA,
    TIMEOUT_LIGHT_TASK,
    TIMEOUT_SCRAPING,
    TIMEOUT_SPARK_JOB,
    DataError,
    DataQualityError,
    InfrastructureError,
    ScrapingError,
)
from utils.run_context import run_ds, run_logical_date
from utils.trino_client import run_query

logger = logging.getLogger(__name__)

LOCAL_TZ = "Africa/Casablanca"


def _record_task_result(context, status: str) -> None:
    ti = context["task_instance"]
    run_id = context["dag_run"].run_id
    exception = context.get("exception")
    error_category = getattr(exception, "category", None) if exception else None
    error_message = getattr(exception, "summary", str(exception)) if exception else None
    try:
        metrics.record_task_run(
            run_id=run_id,
            task_id=ti.task_id,
            status=status,
            start_time=ti.start_date,
            end_time=ti.end_date,
            retries=(ti.try_number - 1) if getattr(ti, "try_number", None) else 0,
            error_category=error_category,
            error_message=error_message,
        )
    except Exception:
        logger.exception("Echec de l'enregistrement des metriques pour la tache %s", ti.task_id)


def _on_success(context):
    _record_task_result(context, "SUCCESS")


def _on_failure(context):
    _record_task_result(context, "FAILED")


default_args = {
    "on_success_callback": _on_success,
    "on_failure_callback": _on_failure,
}

with DAG(
    dag_id="adii_daily_ingestion",
    description="Collecte quotidienne ADII : BADR (append) + scraping -> Bronze -> Silver -> ML -> valeur MAD (pas d'arbitrage, pas de Gold)",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["adii", "daily-ingestion"],
) as dag:

    @task(
        task_id="check_environment",
        retries=RETRY_POLICY_INFRA["retries"],
        retry_delay=RETRY_POLICY_INFRA["retry_delay"],
        retry_exponential_backoff=RETRY_POLICY_INFRA["retry_exponential_backoff"],
        execution_timeout=TIMEOUT_LIGHT_TASK,
    )
    def check_environment(**context):
        """Identical to the retired main_pipeline.py's task of the same
        name - initializes this run's reporting.pipeline_runs row.
        """
        run_id = context["dag_run"].run_id
        logical_date = run_logical_date(context)
        metrics.start_run(run_id, logical_date, dag_id=context["dag_run"].dag_id)

        import config as ingestion_config
        from bronze_ingestion import get_s3_client

        try:
            s3 = get_s3_client()
            s3.head_bucket(Bucket=ingestion_config.MINIO_BUCKET)
        except Exception as exc:
            raise InfrastructureError(
                f"MinIO injoignable a {ingestion_config.MINIO_ENDPOINT_HOST} "
                f"(bucket '{ingestion_config.MINIO_BUCKET}').",
                details=str(exc),
            ) from exc

        try:
            run_query("SELECT 1")
        except Exception as exc:
            raise InfrastructureError("Trino injoignable (SELECT 1 a echoue).", details=str(exc)) from exc

        return {"minio": "ok", "trino": "ok"}

    @task(
        task_id="generate_badr_daily",
        retries=RETRY_POLICY_DATA["retries"],
        retry_delay=RETRY_POLICY_DATA["retry_delay"],
        execution_timeout=TIMEOUT_LIGHT_TASK,
    )
    def generate_badr_daily(_env_ok, **context):
        """NEW task (Etape 1/3). Runs ingestion/badr/generate_badr.py
        --run-date {{ ds }} via subprocess (same pattern as scrape_web's
        own subprocess call to scrape_prices.py - a clean process boundary,
        so generate_badr.py's own argparse never has to coexist with
        Airflow's real sys.argv). INSERT-only, anti-doublon guard already
        inside the script itself - safe to retry.
        """
        ds = run_ds(context)
        result = subprocess.run(
            ["python", "/opt/airflow/ingestion/badr/generate_badr.py", "--run-date", ds],
            capture_output=True,
            text=True,
            timeout=TIMEOUT_LIGHT_TASK.total_seconds(),
        )
        tail = (result.stdout + result.stderr)[-4000:]
        if result.returncode != 0:
            raise DataError(f"generate_badr.py --run-date {ds} a echoue (exit code {result.returncode}).", details=tail)
        return {"run_date": ds, "log_tail": tail}

    @task(
        task_id="ingest_badr",
        retries=RETRY_POLICY_INFRA["retries"],
        retry_delay=RETRY_POLICY_INFRA["retry_delay"],
        execution_timeout=TIMEOUT_LIGHT_TASK,
    )
    def ingest_badr(_badr_generated, **context):
        """Redistributed from main_pipeline.py, adapted for Etape 2's
        partitioned Bronze layout: now passes run_date so ingest_badr()
        writes bronze/badr/date={ds}/badr.parquet (a full current-state
        snapshot) instead of the legacy fixed key.
        """
        import config as ingestion_config
        from bronze_ingestion import get_s3_client
        from bronze_ingestion import ingest_badr as _ingest_badr
        from bronze_ingestion import verify_roundtrip

        ds = run_ds(context)
        s3 = get_s3_client()
        try:
            df = _ingest_badr(s3, ds)
            verify_roundtrip(s3, ingestion_config.MINIO_BUCKET, ingestion_config.bronze_badr_key(ds), df, "BADR")
        except FileNotFoundError as exc:
            raise DataError(f"data/badr.db introuvable : {exc}") from exc
        except Exception as exc:
            raise InfrastructureError("Echec de l'ingestion Bronze BADR (MinIO).", details=str(exc)) from exc

        return {"rows": len(df)}

    @task(
        task_id="scrape_web",
        retries=RETRY_POLICY_INFRA["retries"],
        retry_delay=RETRY_POLICY_INFRA["retry_delay"],
        execution_timeout=TIMEOUT_SCRAPING,
    )
    def scrape_web(_env_ok, **context):
        """Identical to the retired main_pipeline.py's task of the same
        name (see that file's docstring for the completeness-check
        rationale) - unchanged.
        """
        import pandas as pd

        import config as ingestion_config

        result = subprocess.run(
            ["python", "/opt/airflow/ingestion/scraping/scrape_prices.py"],
            capture_output=True,
            text=True,
            timeout=TIMEOUT_SCRAPING.total_seconds(),
        )
        tail = (result.stdout + result.stderr)[-4000:]
        if result.returncode != 0:
            raise ScrapingError(f"scrape_prices.py a echoue (exit code {result.returncode}).", details=tail)

        df = pd.read_csv(ingestion_config.SCRAPING_OUTPUT_CSV)
        counts = df["categorie"].value_counts().to_dict()
        expected_categories = list(ingestion_config.SCRAPING_CATEGORIES.keys())
        counts_full = {cat: int(counts.get(cat, 0)) for cat in expected_categories}

        run_id = context["dag_run"].run_id
        metrics.record_scraping_counts(run_id, counts_full)

        empty = [cat for cat, n in counts_full.items() if n == 0]
        if empty:
            raise ScrapingError(
                f"Categorie(s) vide(s) apres scraping : {', '.join(empty)} "
                f"(repartition complete : {counts_full}). "
                "Prix de reference serait incomplet en aval pour cette categorie.",
                details=tail,
            )

        return {"counts": counts_full, "total": sum(counts_full.values())}

    @task(
        task_id="ingest_bronze_scraping",
        retries=RETRY_POLICY_INFRA["retries"],
        retry_delay=RETRY_POLICY_INFRA["retry_delay"],
        execution_timeout=TIMEOUT_LIGHT_TASK,
    )
    def ingest_bronze_scraping(_scrape_result, **context):
        """Redistributed from main_pipeline.py, adapted for Etape 2's
        partitioned Bronze layout (bronze/scraping/date={ds}/prix_web.parquet).
        """
        import config as ingestion_config
        from bronze_ingestion import get_s3_client
        from bronze_ingestion import ingest_scraping as _ingest_scraping
        from bronze_ingestion import verify_roundtrip

        ds = run_ds(context)
        s3 = get_s3_client()
        try:
            df = _ingest_scraping(s3, ds)
            verify_roundtrip(s3, ingestion_config.MINIO_BUCKET, ingestion_config.bronze_scraping_key(ds), df, "Scraping")
        except FileNotFoundError as exc:
            raise DataError(f"data/prix_web.csv introuvable : {exc}") from exc
        except Exception as exc:
            raise InfrastructureError("Echec de l'ingestion Bronze scraping (MinIO).", details=str(exc)) from exc

        return {"rows": len(df)}

    @task(
        task_id="bronze_to_silver",
        retries=RETRY_POLICY_DATA["retries"],
        retry_delay=RETRY_POLICY_DATA["retry_delay"],
        execution_timeout=TIMEOUT_SPARK_JOB,
    )
    def bronze_to_silver(_badr_result, _bronze_scraping_result, **context):
        """Redistributed from main_pipeline.py, now passes --run-date so
        it reads the partitioned Bronze layout (Etape 2) instead of the
        legacy fixed keys. Silver itself stays a single always-current
        snapshot (unpartitioned) - see spark/jobs/bronze_to_silver.py.
        """
        ds = run_ds(context)
        return {"log_tail": run_spark_job("bronze_to_silver.py", ["--run-date", ds])[-1000:]}

    @task(
        task_id="classification_ngp",
        retries=RETRY_POLICY_DATA["retries"],
        retry_delay=RETRY_POLICY_DATA["retry_delay"],
        execution_timeout=TIMEOUT_LIGHT_TASK,
    )
    def classification_ngp(_silver_result, **context):
        """Redistributed from main_pipeline.py, now passes run_date so
        apply_model.py ALSO writes the partitioned scraping_ml key (Etape
        3 prerequisite for arbitrage.py's period-aware prix_reference).
        """
        import apply_model

        ds = run_ds(context)
        try:
            apply_model.main(ds)
        except FileNotFoundError as exc:
            raise DataError(f"Source ou modele introuvable pour la classification NGP : {exc}") from exc
        except ValueError as exc:
            raise DataQualityError(f"Controle de coherence de apply_model.py echoue : {exc}") from exc
        except Exception as exc:
            raise InfrastructureError("Echec de la classification NGP (MinIO).", details=str(exc)) from exc

        return {"status": "ok"}

    @task(
        task_id="matching_produits",
        retries=RETRY_POLICY_DATA["retries"],
        retry_delay=RETRY_POLICY_DATA["retry_delay"],
        execution_timeout=TIMEOUT_LIGHT_TASK,
    )
    def matching_produits(_classification_result):
        """Identical to the retired main_pipeline.py's task of the same
        name - unchanged. Stays daily (not moved to arbitrage.py): it is a
        CODE_NGP-level informational reconciliation, not an input to
        prix_reference/ratio_unitaire (those read scraping_ml directly).
        """
        import prepare_matching

        try:
            prepare_matching.main()
        except FileNotFoundError as exc:
            raise DataError(f"Source introuvable pour le matching : {exc}") from exc
        except ValueError as exc:
            raise DataQualityError(f"Controle de coherence de prepare_matching.py echoue : {exc}") from exc
        except Exception as exc:
            raise InfrastructureError("Echec du matching produits (MinIO).", details=str(exc)) from exc

        return {"status": "ok"}

    @task(
        task_id="taux_change",
        retries=RETRY_POLICY_DATA["retries"],
        retry_delay=RETRY_POLICY_DATA["retry_delay"],
        execution_timeout=TIMEOUT_LIGHT_TASK,
    )
    def taux_change(_env_ok):
        """Identical to the retired main_pipeline.py's task of the same
        name - unchanged.
        """
        import build_taux_change

        try:
            build_taux_change.main()
        except ValueError as exc:
            raise DataQualityError(f"Controle de coherence de build_taux_change.py echoue : {exc}") from exc
        except Exception as exc:
            raise InfrastructureError("Echec de la construction du taux de change (MinIO).", details=str(exc)) from exc

        return {"status": "ok"}

    @task(
        task_id="valeur_mad_quantite",
        retries=RETRY_POLICY_DATA["retries"],
        retry_delay=RETRY_POLICY_DATA["retry_delay"],
        execution_timeout=TIMEOUT_SPARK_JOB,
    )
    def valeur_mad_quantite(_silver_result, _taux_change_result):
        """Identical to the retired main_pipeline.py's task of the same
        name - unchanged. Last task of the former pipeline this DAG keeps;
        prix_reference onward moves to arbitrage.py.
        """
        return {"log_tail": run_spark_job("badr_valeur_prep.py")[-1000:]}

    @task(
        task_id="data_quality_daily",
        retries=RETRY_POLICY_DATA["retries"],
        retry_delay=RETRY_POLICY_DATA["retry_delay"],
        execution_timeout=TIMEOUT_LIGHT_TASK,
    )
    def data_quality_daily(_valeur_result, **context):
        """NEW task (Etape 3) - not a 1:1 port. The old data_quality_extra
        checked Gold (fct_arbitrage), which this DAG no longer produces.
        Instead checks the one invariant that changed with Etape 1's
        append mode: Silver BADR must contain exactly the CURRENT real
        BADR population, not a stale expectation - the old code hardcoded
        5000 in three places (data_quality_extra/validation_finale/
        collect_metrics), which silently becomes wrong the moment BADR
        grows past 5000.
        """
        import io

        import boto3
        import pandas as pd

        import config as ingestion_config
        from bronze_ingestion import count_badr_declarations

        s3 = boto3.client(
            "s3",
            endpoint_url=ingestion_config.MINIO_ENDPOINT_HOST,
            aws_access_key_id=ingestion_config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=ingestion_config.AWS_SECRET_ACCESS_KEY,
            region_name=ingestion_config.AWS_REGION,
        )
        parts = [
            obj["Key"]
            for page in s3.get_paginator("list_objects_v2").paginate(
                Bucket=ingestion_config.MINIO_BUCKET, Prefix="silver/badr/"
            )
            for obj in page.get("Contents", [])
            if obj["Key"].endswith(".parquet")
        ]
        if not parts:
            raise DataQualityError("Aucun fichier Silver BADR trouve - bronze_to_silver a-t-il bien tourne ?")
        silver_count = sum(
            len(pd.read_parquet(io.BytesIO(s3.get_object(Bucket=ingestion_config.MINIO_BUCKET, Key=k)["Body"].read())))
            for k in parts
        )

        expected = count_badr_declarations()
        if silver_count != expected:
            raise DataQualityError(
                f"Silver BADR a {silver_count} lignes, attendu {expected} (population reelle de data/badr.db) - "
                "des lignes ont peut-etre ete perdues ou dupliquees entre Bronze et Silver."
            )

        return {"silver_badr_rows": silver_count, "expected": expected}

    @task(
        task_id="collect_metrics",
        trigger_rule="all_done",
        retries=0,
        execution_timeout=TIMEOUT_LIGHT_TASK,
    )
    def collect_metrics(_dq_result, _matching_result, **context):
        """Finalizes reporting.pipeline_runs for THIS DAG's own run_id
        (distinct from arbitrage.py's run_id - separate DAGs, separate
        rows, same reporting.* schema, unchanged). No Gold KPIs here
        (business_metrics/data_quality_runs stay arbitrage.py's job).
        """
        run_id = context["dag_run"].run_id
        dag_run = context["dag_run"]

        all_states = metrics.get_task_states(dag_run.dag_id, run_id)
        states = {tid: s for tid, s in all_states.items() if tid != "collect_metrics"}
        if all(s == "success" for s in states.values()):
            global_status = "SUCCESS"
        elif any(s == "failed" for s in states.values()):
            global_status = "FAILED"
        else:
            global_status = "PARTIAL"

        metrics.finish_run(run_id, global_status, notes=f"task states: {states}")
        return {"global_status": global_status}

    env_ok = check_environment()
    badr_generated = generate_badr_daily(env_ok)
    badr_result = ingest_badr(badr_generated)
    scrape_result = scrape_web(env_ok)
    bronze_scraping_result = ingest_bronze_scraping(scrape_result)
    silver_result = bronze_to_silver(badr_result, bronze_scraping_result)
    classification_result = classification_ngp(silver_result)
    matching_result = matching_produits(classification_result)
    taux_change_result = taux_change(env_ok)
    valeur_result = valeur_mad_quantite(silver_result, taux_change_result)
    dq_result = data_quality_daily(valeur_result)
    collect_metrics(dq_result, matching_result)
