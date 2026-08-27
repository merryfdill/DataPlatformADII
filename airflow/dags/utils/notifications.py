"""Slack notifications: the 07:00 daily report and immediate technical
alerts on critical task failures. Uses the Slack Incoming Webhook
(apache-airflow-providers-slack, already installed in the base image) via
the Airflow Connection "slack_webhook" - not yet configured, the user sets
this up after implementation (no credential can be created on their
behalf). If it is missing or the send fails, every function here logs the
full message and returns without raising - a report that cannot be
delivered must never crash the DAG.
"""

import logging

from utils.config import SLACK_CONN_ID

logger = logging.getLogger(__name__)


def send_slack_message(text: str) -> bool:
    """Sends `text` to the configured Slack webhook. Returns True on
    success, False otherwise - never raises, so a Slack outage can never
    fail the DAG that calls this.
    """
    try:
        from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
    except ImportError:
        logger.warning("apache-airflow-providers-slack indisponible - message non envoye:\n%s", text)
        return False

    try:
        hook = SlackWebhookHook(slack_webhook_conn_id=SLACK_CONN_ID)
        hook.send(text=text)
        return True
    except Exception as exc:
        logger.warning(
            "Echec de l'envoi Slack (connexion '%s' non configuree ou erreur reseau) : %s\n"
            "Message qui aurait ete envoye :\n%s",
            SLACK_CONN_ID,
            exc,
            text,
        )
        return False


def send_technical_alert(task_id: str, error_summary: str, error_category: str, run_id: str) -> None:
    """Immediate short alert for a CRITICAL task failure - distinct from
    the 07:00 daily report. Called from on_failure_callback in
    main_pipeline.py, only on tasks explicitly marked critical.
    """
    text = (
        ":rotating_light: *Alerte technique ADII* — tache critique en echec\n"
        f"*Tache* : `{task_id}`\n"
        f"*Categorie* : {error_category}\n"
        f"*Run* : `{run_id}`\n"
        f"*Resume* : {error_summary}"
    )
    send_slack_message(text)
