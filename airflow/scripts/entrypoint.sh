#!/usr/bin/env bash
# One-shot initialization for Airflow: run database migrations and
# ensure the admin user exists. Used by the airflow-init service only;
# the webserver/scheduler containers use the base image entrypoint.
set -euo pipefail

airflow db migrate

airflow users create \
    --username "${_AIRFLOW_WWW_USER_USERNAME:-admin}" \
    --password "${_AIRFLOW_WWW_USER_PASSWORD:-admin}" \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    || echo "Admin user already exists, skipping."
