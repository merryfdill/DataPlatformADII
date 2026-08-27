"""Shared constants for the ADII Airflow DAGs: container names, the exact
spark-submit command, dbt invocation details, and the Postgres reporting
schema name.

The spark-submit flag list below is copied VERBATIM from the "Run with"
block in each Spark job's own module docstring (identical across all 7
jobs - e.g. spark/jobs/arbitrage_gold.py:44-51,
spark/jobs/bronze_to_silver.py:12-19, spark/jobs/register_gold_iceberg.py:
21-28) - never reconstructed from memory. If a job's own docstring ever
changes this list, copy it again from there rather than hand-editing this
constant.

The access/secret key VALUES are read from AWS_ACCESS_KEY_ID /
AWS_SECRET_ACCESS_KEY (confirmed set in the airflow-scheduler container -
already injected by docker-compose.yml's x-airflow-common block, same env
vars the rest of the project already uses) rather than hardcoded here -
unlike the job docstrings (prose documentation, not committed as a live
credential path), this module is executable code that builds the real
command, so the literal "minioadmin" string must not live in it.
"""

import os

SPARK_CONTAINER = "dataplatformadii-spark-iceberg-1"
DBT_CONTAINER = "dataplatformadii-dbt-1"
DBT_WORKDIR = "/usr/app/dbt"
SPARK_JOBS_CONTAINER_PATH = "/home/iceberg/jobs"

# Verbatim from every job's docstring "Run with" block, EXCEPT the
# access.key/secret.key values, which are substituted at call time in
# build_spark_submit_command() from AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY -
# {} placeholders here, never a literal credential.
SPARK_SUBMIT_BASE_ARGS_TEMPLATE = [
    "--packages",
    "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "--conf",
    "spark.hadoop.fs.s3a.endpoint=http://minio:9000",
    "--conf",
    "spark.hadoop.fs.s3a.access.key={access_key}",
    "--conf",
    "spark.hadoop.fs.s3a.secret.key={secret_key}",
    "--conf",
    "spark.hadoop.fs.s3a.path.style.access=true",
    "--conf",
    "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
    "--conf",
    "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
]

# register_gold_iceberg.py needs ONE flag beyond what its own docstring
# shows. Not written in that docstring - it is documented as a runtime fix
# in docs/dbt_gold.md ("Un correctif mineur a ete necessaire au lancement...
# --conf spark.sql.catalog.iceberg.s3.path-style-access=true, passe en
# ligne de commande, aucun fichier image modifie") after the job hit
# `UnknownHostException: datalake.minio` - Iceberg's own S3 client defaults
# to virtual-hosted-style addressing unless told otherwise, unlike the s3a
# reads above which already set path-style-access=true for the Hadoop side.
SPARK_SUBMIT_EXTRA_ARGS_BY_JOB = {
    "register_gold_iceberg.py": [
        "--conf",
        "spark.sql.catalog.iceberg.s3.path-style-access=true",
    ],
}

# Real pipeline order (see plan §"Constat cle sur la granularite des
# etapes" - classification NGP runs before matching, register_gold_iceberg
# is a required bridge step absent from the generic 17-step list).
SPARK_JOB_ORDER = [
    "bronze_to_silver.py",
    "badr_valeur_prep.py",
    "prix_reference.py",
    "ratio_unitaire.py",
    "arbitrage_gold.py",
    "register_gold_iceberg.py",
]


def build_spark_submit_command(job_filename: str, extra_script_args: list[str] | None = None) -> str:
    """Builds the full spark-submit command string for one job, using the
    verbatim base flags (credentials substituted from environment, never
    hardcoded) plus any job-specific extra flags.

    extra_script_args (optional): appended AFTER the job path, i.e. passed
    to the job's own argparse (e.g. ["--date-debut", "2026-09-01",
    "--date-fin", "2026-09-15"] for prix_reference.py), not to spark-submit
    itself - the spark-submit flags above are never affected by this.
    Additive: omitted (the default), the command is byte-identical to
    before - every existing caller (bronze_to_silver.py, arbitrage_gold.py,
    etc.) is unaffected.
    """
    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if not access_key or not secret_key:
        raise RuntimeError(
            "AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY manquantes dans l'environnement - "
            "impossible de construire la commande spark-submit sans hardcoder les identifiants MinIO."
        )

    args = [
        arg.format(access_key=access_key, secret_key=secret_key) for arg in SPARK_SUBMIT_BASE_ARGS_TEMPLATE
    ]
    args += SPARK_SUBMIT_EXTRA_ARGS_BY_JOB.get(job_filename, [])
    job_path = f"{SPARK_JOBS_CONTAINER_PATH}/{job_filename}"
    command = "spark-submit " + " ".join(args) + " " + job_path
    if extra_script_args:
        command += " " + " ".join(extra_script_args)
    return command


REPORTING_SCHEMA = "reporting"
SLACK_CONN_ID = "slack_webhook"

# CODE_NGP scope, matching config.SCRAPING_CATEGORY_TO_NGP8 in
# ingestion/config.py (Phase 2.8) - duplicated here (not imported) since
# this module must stay import-safe even before /opt/airflow/ingestion is
# on sys.path.
NGP_CATEGORY_LABELS = {
    "85171300": "Smartphone",
    "84713000": "PC Portable",
    "85287200": "Televiseur",
}
