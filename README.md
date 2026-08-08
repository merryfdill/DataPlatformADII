# DataPlatformADII

MVP Data Engineering platform for customs declaration valuation
and risk detection.

## Architecture

BADR + Web Scraping
        ↓
     Airflow
        ↓
      Bronze
        ↓
      MinIO
        ↓
      Spark
        ↓
     Silver
        ↓
     Iceberg
        ↓
      dbt
        ↓
      Gold
        ↓
     Trino
        ↓
     Grafana

## Main technologies

- Apache Airflow
- Apache Spark / PySpark
- MinIO
- Apache Iceberg
- dbt
- Trino
- Grafana
- Docker

## Structure

- airflow/          Orchestration
- ingestion/        BADR and web ingestion
- spark/            Data processing
- dbt/              Transformations and tests
- infrastructure/   MinIO, Iceberg and Trino
- monitoring/       Grafana
- data/             Local temporary data
- notebooks/        Exploration and ML
