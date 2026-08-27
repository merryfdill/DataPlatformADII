"""Task-context accessors that survive a manually-triggered run.

Airflow 3's task_runner only adds ``logical_date`` / ``ds`` / ``ts`` /
``data_interval_*`` to the task context when ``dag_run.logical_date`` is not
None (``if logical_date := coerce_datetime(dag_run.logical_date):`` in
airflow/sdk/execution_time/task_runner.py). A run started from the UI "Trigger"
button or ``airflow dags trigger`` with no ``--logical-date`` has
``logical_date = NULL``, so ``context["logical_date"]`` and ``context["ds"]``
raise ``KeyError`` - the task dies with no traceback in its own log. Scheduled
runs are unaffected (the cron tick always supplies a logical date).

Both helpers derive from the SAME timestamp: ``logical_date`` when present,
else ``dag_run.run_after`` (always set, and traceable to this exact run - not
``now()``, which would drift from the run it belongs to). ``run_ds`` formats
that same value, so ``logical_date`` and ``ds`` can never diverge.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any


def run_logical_date(context: dict[str, Any]) -> datetime:
    """This run's logical date, or its ``run_after`` when triggered without one."""
    return context.get("logical_date") or context["dag_run"].run_after


def run_ds(context: dict[str, Any]) -> str:
    """``YYYY-MM-DD`` string for the same timestamp as :func:`run_logical_date`."""
    return run_logical_date(context).strftime("%Y-%m-%d")
