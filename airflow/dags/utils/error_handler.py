"""Typed exceptions for the ADII pipeline DAGs.

Airflow's own retry/failure machinery only needs *an* exception to be
raised, but the 07:00 report needs to say *what kind* of failure happened
(temporaire / infrastructure / donnees / scraping / pyspark / minio_iceberg
/ dbt / data_quality) in one short line, never a full stack trace (see
utils/report_generator.py). Every PipelineError subclass carries that
category plus a short human-readable summary, so the exception itself is
both what Airflow uses to mark the task FAILED and what the report reads.
"""

from datetime import timedelta


class PipelineError(Exception):
    """Base class for every custom pipeline error.

    `category` is one of the error categories requested for the daily
    report. `retryable` documents whether retrying this specific failure
    kind is worth attempting - it does not change Airflow's behavior by
    itself, it is only a note for whoever tunes each task's retry policy in
    main_pipeline.py (see RETRY_POLICY_* below).
    """

    category = "inconnu"
    retryable = False

    def __init__(self, summary: str, *, details: str | None = None):
        self.summary = summary
        self.details = details
        super().__init__(summary)


class TemporaryError(PipelineError):
    """Transient failure (network blip, container briefly unreachable) - safe to retry."""

    category = "temporaire"
    retryable = True


class InfrastructureError(PipelineError):
    """MinIO/Trino/Iceberg/Docker unreachable or misconfigured."""

    category = "infrastructure"
    retryable = True


class DataError(PipelineError):
    """The data itself is the problem (missing file, unexpected schema, empty result) - retrying will not help."""

    category = "donnees"
    retryable = False


class ScrapingError(PipelineError):
    """Web scraping failure - a live third-party site can be flaky, so retryable."""

    category = "scraping"
    retryable = True


class SparkJobError(PipelineError):
    """A spark-submit process exited non-zero. Not retryable by default: a
    Spark job that crashed on this data will crash again unless the
    underlying cause is fixed - see docker_exec.py, which is what raises
    this after explicitly checking exit_code.
    """

    category = "pyspark"
    retryable = False


class IcebergError(PipelineError):
    """Iceberg REST catalog / SQLite-backend contention (the same class of
    transient ICEBERG_COMMIT_ERROR seen in Phase 2.22) - retryable.
    """

    category = "minio_iceberg"
    retryable = True


class DbtError(PipelineError):
    """`dbt run` or `dbt test` exited non-zero (dbt test also exits non-zero
    on a failed assertion, not just a crash) - see docker_exec.py.
    """

    category = "dbt"
    retryable = False


class DataQualityError(PipelineError):
    """A data-quality check outside of dbt's own tests failed (see
    utils/config.py DATA_QUALITY queries)."""

    category = "data_quality"
    retryable = False


# Retry/timeout policies, applied explicitly per task in main_pipeline.py -
# not automatic. Infra-flavored tasks (network/container calls) get retries
# with backoff; deterministic business-logic tasks get at most one retry
# (only useful for a transient container hiccup, not a real bug).
RETRY_POLICY_INFRA = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
}
RETRY_POLICY_DATA = {
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

TIMEOUT_LIGHT_TASK = timedelta(minutes=5)
TIMEOUT_SCRAPING = timedelta(minutes=10)
TIMEOUT_SPARK_JOB = timedelta(minutes=20)
TIMEOUT_DBT = timedelta(minutes=10)
