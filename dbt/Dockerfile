FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir dbt-core dbt-trino

WORKDIR /usr/app/dbt
