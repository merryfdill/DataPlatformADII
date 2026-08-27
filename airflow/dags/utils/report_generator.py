"""Builds the formatted Slack message for the 07:00 daily report and
computes the 7-day trend. Reads only what utils/metrics.py already
collected from real Gold/dbt/task-run data (see daily_report.py) - never
invents a number here.
"""

from datetime import datetime, timezone

try:
    from airflow.configuration import conf as airflow_conf
except ImportError:  # pragma: no cover - only relevant outside an Airflow container
    airflow_conf = None


def _airflow_run_url(run_id: str, dag_id: str) -> str:
    base = "http://localhost:8080"
    if airflow_conf is not None:
        try:
            base = airflow_conf.get("webserver", "base_url") or base
        except Exception:
            pass
    return f"{base}/dags/{dag_id}/grid?dag_run_id={run_id}"


def _pct_delta(current, previous) -> str:
    if previous in (None, 0) or current is None:
        return "n/d"
    delta = 100.0 * (current - previous) / previous
    sign = "+" if delta >= 0 else ""
    return f"{sign}{delta:.1f}%"


def build_daily_report(run_info: dict | None, run_details: dict, trend: list[dict]) -> str:
    """run_info: row from reporting.pipeline_runs, or None if no run has
    ever been recorded. run_details: output of metrics.get_run_details().
    trend: output of metrics.get_trend(), most recent row first.
    """
    today = datetime.now(timezone.utc).strftime("%d/%m/%Y")

    if run_info is None:
        return (
            "*DAILY DATA PIPELINE REPORT — ADII*\n"
            f"Date : {today}\n"
            "Pipeline : 03:00\n"
            "Statut global : *AUCUN RUN TROUVE*\n\n"
            "Aucune execution du pipeline n'a encore ete enregistree dans `reporting.pipeline_runs`."
        )

    status = run_info.get("global_status") or "INCONNU"
    run_id = run_info["run_id"]
    # dag_id is NULL for rows written before the Etape 3 migration (single
    # adii_main_pipeline DAG, now retired) - falls back to
    # adii_daily_ingestion, the closer of the two current DAGs to what that
    # old DAG did, rather than pointing at a DAG that no longer exists.
    dag_id = run_info.get("dag_id") or "adii_daily_ingestion"
    lines = [
        "*DAILY DATA PIPELINE REPORT — ADII*",
        f"Date : {today}",
        "Pipeline : 03:00",
        f"Statut global : *{status}*",
        "",
    ]

    task_runs = run_details.get("task_runs") or []
    if task_runs:
        lines.append("*Etapes*")
        for t in task_runs:
            icon = {"SUCCESS": "✅", "FAILED": "❌", "SKIPPED": "⏭️"}.get(t["status"], "•")
            dur = f"{t['duration_seconds']:.0f}s" if t.get("duration_seconds") else "n/d"
            lines.append(f"{icon} `{t['task_id']}` — {t['status']} ({dur})")
        lines.append("")

    biz = run_details.get("business_metrics")
    if biz:
        lines.append("*ARBITRAGE* (source : `iceberg.gold.mart_arbitrage_kpi`)")
        lines.append(f"Total declarations analysees : {biz.get('nb_declarations')}")
        lines.append(
            f"NORMAL : {biz.get('nb_normal')} ({biz.get('pct_normal', 0):.1f}%) | "
            f"MINORE : {biz.get('nb_minore')} ({biz.get('pct_minore', 0):.1f}%) | "
            f"MAJORE : {biz.get('nb_majore')} ({biz.get('pct_majore', 0):.1f}%)"
        )
        lines.append(
            f"Ratio moyen : {biz.get('ratio_moyen', 0):.3f} | Ratio median : {biz.get('ratio_median', 0):.3f} | "
            f"min : {biz.get('ratio_min', 0):.3f} | max : {biz.get('ratio_max', 0):.3f}"
        )
        lines.append(
            f"Valeur totale MAD : {biz.get('valeur_totale_mad', 0):,.0f} | "
            f"Quantite totale : {biz.get('quantite_totale', 0)}"
        )
        if biz.get("nb_hors_perimetre"):
            lines.append(
                "Hors perimetre (CODE_NGP non couvert par le scraping, pas une erreur) : "
                f"{biz['nb_hors_perimetre']}"
            )
        lines.append("")

    dq = run_details.get("data_quality")
    if dq:
        lines.append("*DATA QUALITY*")
        lines.append(f"dbt : {dq.get('dbt_tests_passed', 0)} tests OK / {dq.get('dbt_tests_failed', 0)} en echec")
        issues = []
        for label, key in [
            ("doublons", "duplicates"),
            ("valeurs nulles critiques", "nulls_critical"),
            ("CODE_NGP invalides", "invalid_code_ngp"),
            ("prix invalides", "invalid_prices"),
            ("ratios invalides", "invalid_ratios"),
            ("produits non apparies", "unmatched_products"),
        ]:
            val = dq.get(key)
            if val:
                issues.append(f"⚠ {val} {label}")
        lines.extend(issues if issues else ["✓ Aucune anomalie de qualite detectee sur ces controles."])
        lines.append("")

    failed = [t for t in task_runs if t["status"] == "FAILED"]
    lines.append("*ERREURS / PANNES*")
    if not failed:
        lines.append("✓ Aucune erreur detectee.")
    else:
        for t in failed:
            cat = t.get("error_category") or "inconnu"
            msg = (t.get("error_message") or "")[:200]
            lines.append(f"⚠ `{t['task_id']}` ({cat}) : {msg}")
    lines.append("")

    lines.append(f"\U0001f50e <{_airflow_run_url(run_id, dag_id)}|Voir les details>")
    lines.append("")

    if len(trend) >= 2:
        latest, previous = trend[0], trend[-1]
        lines.append("*7-DAY TREND*")
        lines.append(
            f"Declarations traitees : {_pct_delta(latest.get('nb_declarations'), previous.get('nb_declarations'))}"
        )
        lines.append(f"MINORE : {_pct_delta(latest.get('nb_minore'), previous.get('nb_minore'))}")
        lines.append(f"MAJORE : {_pct_delta(latest.get('nb_majore'), previous.get('nb_majore'))}")

    return "\n".join(lines)
