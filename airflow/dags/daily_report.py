"""ADII daily report - independent 07:00 Slack summary of the most recent
adii_main_pipeline run, whatever its outcome.

No ExternalTaskSensor, no cross-DAG trigger rule, no dependency at all on
adii_main_pipeline's own success/failure: a failed or delayed 03:00 run
must never prevent this report from being sent (explicit requirement).
This DAG only reads what utils/metrics.py already collected in the
reporting schema - it never recomputes or invents a number.
"""

import logging

import pendulum

from airflow.sdk import DAG, task

from utils import metrics
from utils.error_handler import RETRY_POLICY_INFRA, TIMEOUT_LIGHT_TASK
from utils.notifications import send_slack_message
from utils.report_generator import build_daily_report

logger = logging.getLogger(__name__)

LOCAL_TZ = "Africa/Casablanca"

with DAG(
    dag_id="adii_daily_report",
    description="Rapport quotidien Slack (07:00) - independant de adii_main_pipeline, envoye meme si le run de 03:00 a echoue",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    tags=["adii", "reporting"],
) as dag:

    @task(
        task_id="fetch_latest_run",
        retries=RETRY_POLICY_INFRA["retries"],
        retry_delay=RETRY_POLICY_INFRA["retry_delay"],
        retry_exponential_backoff=RETRY_POLICY_INFRA["retry_exponential_backoff"],
        execution_timeout=TIMEOUT_LIGHT_TASK,
    )
    def fetch_latest_run():
        """Most recent row of reporting.pipeline_runs, whatever its
        global_status - this DAG has no dependency on adii_main_pipeline
        having succeeded, or even having run today.
        """
        return metrics.get_latest_run()

    @task(
        task_id="compute_trend",
        retries=RETRY_POLICY_INFRA["retries"],
        retry_delay=RETRY_POLICY_INFRA["retry_delay"],
        retry_exponential_backoff=RETRY_POLICY_INFRA["retry_exponential_backoff"],
        execution_timeout=TIMEOUT_LIGHT_TASK,
    )
    def compute_trend():
        return metrics.get_trend(days=7)

    @task(
        task_id="build_report",
        retries=RETRY_POLICY_INFRA["retries"],
        retry_delay=RETRY_POLICY_INFRA["retry_delay"],
        retry_exponential_backoff=RETRY_POLICY_INFRA["retry_exponential_backoff"],
        execution_timeout=TIMEOUT_LIGHT_TASK,
    )
    def build_report(run_info, trend):
        run_details = (
            metrics.get_run_details(run_info["run_id"])
            if run_info
            else {"task_runs": [], "business_metrics": None, "data_quality": None}
        )
        return build_daily_report(run_info, run_details, trend)

    @task(task_id="send_slack", retries=0, execution_timeout=TIMEOUT_LIGHT_TASK)
    def send_slack(report_text: str):
        """send_slack_message() never raises - a Slack outage or a
        not-yet-configured slack_webhook connection must not fail this DAG.
        The full text is logged either way (see utils/notifications.py).
        """
        sent = send_slack_message(report_text)
        if not sent:
            logger.warning("Rapport non envoye a Slack (voir le WARNING precedent) - loggue en entier ci-dessus.")
        return {"sent": sent}

    run_info = fetch_latest_run()
    trend = compute_trend()
    report_text = build_report(run_info, trend)
    send_slack(report_text)
