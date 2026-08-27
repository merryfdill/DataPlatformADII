"""Executes commands inside already-running project containers
(spark-iceberg, dbt) via the Docker SDK - the automated equivalent of the
manual `docker exec <container> ...` used throughout every previous phase
of this project. Requires /var/run/docker.sock mounted into this
container (airflow-scheduler only, see docker-compose.yml) - confirmed
working (docker.from_env() + exec_run against spark-iceberg and dbt, no
permission issue) before this file was written.

CRITICAL: container.exec_run() does NOT raise on a non-zero exit code
inside the container - a spark-submit or dbt run that crashes still
returns *normally* from exec_run(), just with result.exit_code != 0. Every
function below checks that explicitly and raises a typed error
(SparkJobError/DbtError). Without this check, a broken Spark job would be
silently reported as a successful Airflow task, and Gold could be
silently stale or wrong while the DAG shows green.
"""

import logging

import docker
import docker.errors

from utils.config import (
    DBT_CONTAINER,
    DBT_WORKDIR,
    SPARK_CONTAINER,
    build_spark_submit_command,
)
from utils.error_handler import DbtError, InfrastructureError, SparkJobError

logger = logging.getLogger(__name__)

# Airflow task logs stay readable - only the tail of a (potentially huge)
# Spark log is kept in the exception/log, never the full output.
_LOG_TAIL_CHARS = 4000


def _get_client() -> docker.DockerClient:
    try:
        return docker.from_env()
    except Exception as exc:
        raise InfrastructureError(
            "Impossible de se connecter au demon Docker (socket non accessible).",
            details=str(exc),
        ) from exc


def _exec_in_container(container_name: str, command: str, *, workdir: str | None = None) -> tuple[str, int]:
    client = _get_client()
    try:
        container = client.containers.get(container_name)
    except docker.errors.NotFound as exc:
        raise InfrastructureError(
            f"Conteneur '{container_name}' introuvable - la stack est-elle demarree ?",
            details=str(exc),
        ) from exc

    logger.info("docker exec %s: %s", container_name, command)
    result = container.exec_run(command, workdir=workdir, demux=False)
    output = result.output.decode(errors="replace") if result.output else ""
    exit_code = result.exit_code

    logger.info("Exit code: %s", exit_code)
    logger.info("Output (tail):\n%s", output[-_LOG_TAIL_CHARS:])

    return output, exit_code


def run_spark_job(job_filename: str, extra_script_args: list[str] | None = None) -> str:
    """Runs one Spark job via spark-submit inside the spark-iceberg
    container. Raises SparkJobError if the job process itself exits
    non-zero - exec_run() alone would NOT have caught this.

    extra_script_args: optional list forwarded to build_spark_submit_command
    (e.g. date-range params for prix_reference.py) - omitted, every existing
    caller is unaffected.
    """
    command = build_spark_submit_command(job_filename, extra_script_args)
    output, exit_code = _exec_in_container(SPARK_CONTAINER, command)

    if exit_code != 0:
        raise SparkJobError(
            f"spark-submit a echoue pour {job_filename} (exit code {exit_code}).",
            details=output[-_LOG_TAIL_CHARS:],
        )
    return output


def run_dbt(subcommand: str) -> str:
    """Runs `dbt <subcommand> --threads 1` inside the dbt container. Raises
    DbtError if dbt exits non-zero (this also catches `dbt test` failures,
    since dbt exits non-zero when any test fails, not only on a crash).
    """
    command = f"dbt {subcommand} --threads 1"
    output, exit_code = _exec_in_container(DBT_CONTAINER, command, workdir=DBT_WORKDIR)

    if exit_code != 0:
        raise DbtError(
            f"'dbt {subcommand}' a echoue (exit code {exit_code}).",
            details=output[-_LOG_TAIL_CHARS:],
        )
    return output
