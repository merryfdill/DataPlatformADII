"""ADII arbitrage - Etape 3 (daily-simulation phase): the second half of
the former adii_main_pipeline (see main_pipeline.py.bak), redistributed
into its own DAG, triggered MANUALLY (schedule=None) over an explicit
[date_debut, date_fin] period rather than recomputed blindly every night.

Why manual, why a period: the arbitrage verdict is an ABSOLUTE threshold
(RATIO_UNITAIRE vs 1 +/- ARBITRAGE_SEUIL_*_PCT, default 10% - see
spark/jobs/arbitrage_gold.py and docs/arbitrage_gold.md), so it no longer
shifts with the population size. But it still depends on PRIX_REFERENCE,
which is the median of the scraped prices FOR [date_debut, date_fin] and
moves from one run to the next. Running this nightly on a growing BADR
would therefore still reclassify already-judged declarations. A manual,
period-scoped trigger makes each arbitrage run an explicit, reproducible
verdict over a defined population - the customs analogy this project has
used throughout: "la Douane ne rejuge pas une declaration".

date_debut/date_fin default to the full historical BADR range (2024-08-15
-> today). NOTE: the 2026-08-28 rule change (P10/P90 -> absolute 10%
threshold) reclassifies every historical declaration - the old
338/270/34/34 non-regression baseline is void; a new baseline is
established from the first run under the new rule.

Reuses the same utils/ package as daily_ingestion.py and the retired
main_pipeline.py - nothing in utils/ changed for this split.
"""

import logging
import sys
from datetime import timedelta

import pendulum

from airflow.sdk import DAG, task

sys.path.insert(0, "/opt/airflow/ingestion")
sys.path.insert(0, "/opt/airflow/ingestion/ml")

from utils import metrics
from utils.docker_exec import run_dbt, run_spark_job
from utils.error_handler import (
    RETRY_POLICY_DATA,
    TIMEOUT_DBT,
    TIMEOUT_LIGHT_TASK,
    TIMEOUT_SPARK_JOB,
    DataQualityError,
    DbtError,
)
from utils.run_context import run_logical_date
from utils.trino_client import run_query

logger = logging.getLogger(__name__)

LOCAL_TZ = "Africa/Casablanca"

# Full historical BADR range - see module docstring. Recomputed at DAG-parse
# time (not frozen at deploy time); a real trigger can always override both.
_HISTORICAL_DATE_DEBUT = "2024-08-15"


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
    dag_id="adii_arbitrage",
    description="Arbitrage ADII (manuel, periode explicite) : prix_reference -> ratio_unitaire -> NORMAL/MINORE/MAJORE -> Gold -> dbt",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={
        "date_debut": _HISTORICAL_DATE_DEBUT,
        "date_fin": pendulum.now(LOCAL_TZ).to_date_string(),
    },
    tags=["adii", "arbitrage"],
) as dag:

    @task(
        task_id="check_environment",
        retries=RETRY_POLICY_DATA["retries"],
        retry_delay=RETRY_POLICY_DATA["retry_delay"],
        execution_timeout=TIMEOUT_LIGHT_TASK,
    )
    def check_environment(**context):
        """Same checks as the other two DAGs' check_environment, but this
        DAG has no upstream ingestion step of its own to depend on - it
        reads whatever daily_ingestion.py already produced. Also validates
        date_debut <= date_fin before anything else runs.
        """
        run_id = context["dag_run"].run_id
        logical_date = run_logical_date(context)
        params = context["params"]
        date_debut, date_fin = params["date_debut"], params["date_fin"]
        if date_fin < date_debut:
            raise DataQualityError(f"date_fin ({date_fin}) est avant date_debut ({date_debut}).")

        metrics.start_run(run_id, logical_date, dag_id=context["dag_run"].dag_id)

        import config as ingestion_config
        from bronze_ingestion import get_s3_client

        try:
            s3 = get_s3_client()
            s3.head_bucket(Bucket=ingestion_config.MINIO_BUCKET)
        except Exception as exc:
            from utils.error_handler import InfrastructureError

            raise InfrastructureError(
                f"MinIO injoignable a {ingestion_config.MINIO_ENDPOINT_HOST} "
                f"(bucket '{ingestion_config.MINIO_BUCKET}').",
                details=str(exc),
            ) from exc

        try:
            run_query("SELECT 1")
        except Exception as exc:
            from utils.error_handler import InfrastructureError

            raise InfrastructureError("Trino injoignable (SELECT 1 a echoue).", details=str(exc)) from exc

        return {"date_debut": date_debut, "date_fin": date_fin}

    @task(
        task_id="prix_reference",
        retries=RETRY_POLICY_DATA["retries"],
        retry_delay=RETRY_POLICY_DATA["retry_delay"],
        execution_timeout=TIMEOUT_SPARK_JOB,
    )
    def prix_reference(_env_result, **context):
        """Spark: median/min/mean/max retail price per CODE_NGP, now over
        the WHOLE [date_debut, date_fin] period (Etape 3 requirement) -
        unions every existing silver/scraping_ml/date=.../ partition in
        range, falling back to the legacy single file when none exist yet
        (see spark/jobs/prix_reference.py docstring - guarantees this
        reproduces 338/270/34/34 on the historical range).
        """
        params = context["params"]
        date_debut, date_fin = params["date_debut"], params["date_fin"]
        return {
            "log_tail": run_spark_job(
                "prix_reference.py", ["--date-debut", date_debut, "--date-fin", date_fin]
            )[-1000:]
        }

    @task(
        task_id="ratio_unitaire",
        retries=RETRY_POLICY_DATA["retries"],
        retry_delay=RETRY_POLICY_DATA["retry_delay"],
        execution_timeout=TIMEOUT_SPARK_JOB,
    )
    def ratio_unitaire(_prix_ref_result):
        """Identical to the retired main_pipeline.py's task of the same
        name - unchanged. spark/jobs/ratio_unitaire.py itself is untouched;
        it already reads the full current Silver BADR (valeur_mad_quantite's
        output), so it naturally sees whatever daily_ingestion.py has
        accumulated so far.
        """
        return {"log_tail": run_spark_job("ratio_unitaire.py")[-1000:]}

    @task(
        task_id="arbitrage",
        retries=RETRY_POLICY_DATA["retries"],
        retry_delay=RETRY_POLICY_DATA["retry_delay"],
        execution_timeout=TIMEOUT_SPARK_JOB,
    )
    def arbitrage(_ratio_result):
        """Runs spark/jobs/arbitrage_gold.py: RATIO_UNITAIRE -> NORMAL/
        MINORE/MAJORE via the ABSOLUTE threshold rule (2026-08-28, replaces
        P10/P90). The threshold lives in ARBITRAGE_SEUIL_MINORE_PCT /
        ARBITRAGE_SEUIL_MAJORE_PCT (spark-iceberg env, default 10%); the
        job logs the effective bornes at startup. No arg passed here.
        """
        return {"log_tail": run_spark_job("arbitrage_gold.py")[-1000:]}

    @task(
        task_id="register_gold_iceberg",
        # Wider retry policy than RETRY_POLICY_DATA (used everywhere else in
        # this DAG) - reproduced 3/3 times in testing: this specific task,
        # triggered right after arbitrage_gold.py with no gap, hits a
        # NotFoundException on its Iceberg commit (S3 read-after-write
        # timing against MinIO under load) that a 1-retry/1-minute policy
        # does not clear, while a manual rerun moments later always
        # succeeds. More attempts with more spacing gives that same
        # settling time a chance to happen automatically instead of
        # requiring manual recovery every time.
        retries=3,
        retry_delay=timedelta(minutes=2),
        retry_exponential_backoff=True,
        execution_timeout=TIMEOUT_SPARK_JOB,
    )
    def register_gold_iceberg(_arbitrage_result):
        """Identical to the retired main_pipeline.py's task of the same
        name - unchanged, only the retry policy above differs.
        """
        return {"log_tail": run_spark_job("register_gold_iceberg.py")[-1000:]}

    @task(
        task_id="dbt_run",
        retries=RETRY_POLICY_DATA["retries"],
        retry_delay=RETRY_POLICY_DATA["retry_delay"],
        execution_timeout=TIMEOUT_DBT,
    )
    def dbt_run(_register_result):
        """Identical to the retired main_pipeline.py's task of the same
        name - unchanged.
        """
        return {"log_tail": run_dbt("run")[-1500:]}

    @task(
        task_id="dbt_test",
        retries=RETRY_POLICY_DATA["retries"],
        retry_delay=RETRY_POLICY_DATA["retry_delay"],
        execution_timeout=TIMEOUT_DBT,
    )
    def dbt_test(_dbt_run_result, **context):
        """Identical to the retired main_pipeline.py's task of the same
        name - unchanged.
        """
        import re

        run_id = context["dag_run"].run_id
        failed = False
        try:
            output = run_dbt("test")
        except DbtError as exc:
            output = exc.details or ""
            failed = True

        m = re.search(r"PASS=(\d+)\s+WARN=(\d+)\s+ERROR=(\d+)", output)
        passed, warned, errored = (int(x) for x in m.groups()) if m else (0, 0, 0)
        metrics.record_data_quality(run_id, {"dbt_tests_passed": passed, "dbt_tests_failed": errored + warned})

        if failed:
            raise DbtError(f"dbt test : {errored} test(s) en echec sur {passed + errored + warned}.", details=output[-2000:])
        return {"passed": passed, "warned": warned, "errored": errored}

    @task(
        task_id="data_quality_extra",
        retries=RETRY_POLICY_DATA["retries"],
        retry_delay=RETRY_POLICY_DATA["retry_delay"],
        execution_timeout=TIMEOUT_LIGHT_TASK,
    )
    def data_quality_extra(_dbt_test_result, **context):
        """Identical checks to the retired main_pipeline.py's task of the
        same name, EXCEPT: "hors perimetre" is no longer 5000 - count(*)
        (hardcoded, wrong the moment BADR grows past 5000) but
        count_badr_declarations(date_debut, date_fin) - count(*) - the
        real population of THIS run's period, minus what made it into Gold.
        """
        from bronze_ingestion import count_badr_declarations

        run_id = context["dag_run"].run_id
        params = context["params"]
        date_debut, date_fin = params["date_debut"], params["date_fin"]

        dq = run_query(
            """
            SELECT
                (SELECT count(*) - count(DISTINCT badr_id) FROM fct_arbitrage) AS duplicates,
                (SELECT count(*) FROM fct_arbitrage
                    WHERE badr_id IS NULL OR code_ngp IS NULL OR ratio_unitaire IS NULL OR arbitrage IS NULL) AS nulls_critical,
                (SELECT count(*) FROM fct_arbitrage
                    WHERE code_ngp NOT IN ('85171300','84713000','85287200')) AS invalid_code_ngp,
                (SELECT count(*) FROM fct_arbitrage WHERE prix_reference <= 0 OR valeur_mad <= 0) AS invalid_prices,
                (SELECT count(*) FROM fct_arbitrage WHERE ratio_unitaire <= 0) AS invalid_ratios
            """
        )[0]

        population = count_badr_declarations(date_debut, date_fin)
        gold_rows = run_query("SELECT count(*) AS n FROM fct_arbitrage")[0]["n"]
        rows_out_of_scope = population - gold_rows

        dq_full = {
            "duplicates": dq["duplicates"],
            "nulls_critical": dq["nulls_critical"],
            "invalid_code_ngp": dq["invalid_code_ngp"],
            "invalid_prices": dq["invalid_prices"],
            "invalid_ratios": dq["invalid_ratios"],
            "unmatched_products": 0,
            "rows_out_of_scope": rows_out_of_scope,
        }

        if any(v > 0 for k, v in dq_full.items() if k != "rows_out_of_scope"):
            raise DataQualityError(f"Anomalies de qualite detectees : {dq_full}")

        metrics.record_data_quality(run_id, dq_full)
        return dq_full

    @task(
        task_id="validation_finale",
        retries=0,
        execution_timeout=TIMEOUT_LIGHT_TASK,
    )
    def validation_finale(_dq_result, **context):
        """Sanity gate: Gold must be non-empty and within a plausible order
        of magnitude - now bounded by the REAL population of this run's
        period (count_badr_declarations(date_debut, date_fin)), not a
        hardcoded 5000.
        """
        from bronze_ingestion import count_badr_declarations

        params = context["params"]
        date_debut, date_fin = params["date_debut"], params["date_fin"]
        population = count_badr_declarations(date_debut, date_fin)

        count = run_query("SELECT count(*) AS n FROM fct_arbitrage")[0]["n"]
        if count == 0:
            raise DataQualityError("Gold (fct_arbitrage) est vide - echec de validation finale.")
        if count > population:
            raise DataQualityError(
                f"Gold (fct_arbitrage) a {count} lignes, plus que les {population} declarations BADR "
                f"de la periode {date_debut}->{date_fin} - incoherent."
            )
        return {"gold_rows": count, "population": population}

    @task(
        task_id="collect_metrics",
        trigger_rule="all_done",
        retries=0,
        execution_timeout=TIMEOUT_LIGHT_TASK,
    )
    def collect_metrics(_validation_result, **context):
        """Identical logic to the retired main_pipeline.py's task of the
        same name, EXCEPT: nb_hors_perimetre uses
        count_badr_declarations(date_debut, date_fin) instead of a
        hardcoded 5000.
        """
        from bronze_ingestion import count_badr_declarations

        run_id = context["dag_run"].run_id
        logical_date = run_logical_date(context)
        params = context["params"]
        date_debut, date_fin = params["date_debut"], params["date_fin"]
        dag_run = context["dag_run"]

        all_states = metrics.get_task_states(dag_run.dag_id, run_id)
        states = {tid: s for tid, s in all_states.items() if tid != "collect_metrics"}
        critical_tasks = {"arbitrage", "register_gold_iceberg", "dbt_run", "dbt_test"}
        if all(s == "success" for s in states.values()):
            global_status = "SUCCESS"
        elif any(states.get(t) == "failed" for t in critical_tasks):
            global_status = "FAILED"
        else:
            global_status = "PARTIAL"

        try:
            population = count_badr_declarations(date_debut, date_fin)
            kpi = run_query("SELECT * FROM mart_arbitrage_kpi")[0]
            metrics.record_business_metrics(run_id, logical_date, {
                "nb_declarations": kpi["nb_declarations"],
                "nb_normal": kpi["nb_normal"],
                "nb_minore": kpi["nb_minore"],
                "nb_majore": kpi["nb_majore"],
                "pct_normal": float(kpi["pct_normal"]),
                "pct_minore": float(kpi["pct_minore"]),
                "pct_majore": float(kpi["pct_majore"]),
                "ratio_moyen": kpi["ratio_moyen"],
                "ratio_median": kpi["ratio_median"],
                "ratio_min": run_query("SELECT min(ratio_unitaire) AS v FROM fct_arbitrage")[0]["v"],
                "ratio_max": run_query("SELECT max(ratio_unitaire) AS v FROM fct_arbitrage")[0]["v"],
                "valeur_declaree_moyenne": run_query("SELECT avg(valeur_mad) AS v FROM fct_arbitrage")[0]["v"],
                "prix_reference_moyen": run_query("SELECT avg(prix_reference) AS v FROM fct_arbitrage")[0]["v"],
                "valeur_totale_mad": kpi["valeur_totale_mad"],
                "quantite_totale": kpi["quantite_totale"],
                "nb_hors_perimetre": population - kpi["nb_declarations"],
                "nb_sans_code_ngp": 0,
            })
        except Exception:
            logger.exception("Impossible de lire les KPI Gold (Gold probablement indisponible sur ce run en echec).")

        metrics.finish_run(run_id, global_status, notes=f"periode {date_debut}->{date_fin} | task states: {states}")
        return {"global_status": global_status}

    env_result = check_environment()
    prix_ref_result = prix_reference(env_result)
    ratio_result = ratio_unitaire(prix_ref_result)
    arbitrage_result = arbitrage(ratio_result)
    register_result = register_gold_iceberg(arbitrage_result)
    dbt_run_result = dbt_run(register_result)
    dbt_test_result = dbt_test(dbt_run_result)
    dq_result = data_quality_extra(dbt_test_result)
    validation_result = validation_finale(dq_result)
    collect_metrics(validation_result)
