#!/usr/bin/env bash

set -e

ROOT="./"

echo "Creating DataPlatformADII structure..."

# ============================================================
# Directories
# ============================================================

mkdir -p "$ROOT"/airflow/dags
mkdir -p "$ROOT"/airflow/scripts

mkdir -p "$ROOT"/ingestion/badr
mkdir -p "$ROOT"/ingestion/scraping

mkdir -p "$ROOT"/spark/conf
mkdir -p "$ROOT"/spark/jobs

mkdir -p "$ROOT"/dbt/models/staging
mkdir -p "$ROOT"/dbt/models/marts
mkdir -p "$ROOT"/dbt/tests

mkdir -p "$ROOT"/infrastructure/minio
mkdir -p "$ROOT"/infrastructure/iceberg
mkdir -p "$ROOT"/infrastructure/trino/catalog

mkdir -p "$ROOT"/monitoring/grafana/provisioning/datasources
mkdir -p "$ROOT"/monitoring/grafana/provisioning/dashboards
mkdir -p "$ROOT"/monitoring/grafana/dashboards

mkdir -p "$ROOT"/data
mkdir -p "$ROOT"/notebooks

# ============================================================
# Root files
# ============================================================

touch "$ROOT"/docker-compose.yml
touch "$ROOT"/.env.example
touch "$ROOT"/.gitignore
touch "$ROOT"/README.md

# ============================================================
# Airflow
# ============================================================

touch "$ROOT"/airflow/Dockerfile
touch "$ROOT"/airflow/requirements.txt
touch "$ROOT"/airflow/dags/douane_pipeline.py
touch "$ROOT"/airflow/scripts/entrypoint.sh

# ============================================================
# Ingestion
# ============================================================

touch "$ROOT"/ingestion/config.py
touch "$ROOT"/ingestion/badr/generate_badr.py
touch "$ROOT"/ingestion/scraping/scrape_prices.py
touch "$ROOT"/ingestion/bronze_ingestion.py

# ============================================================
# Spark
# ============================================================

touch "$ROOT"/spark/Dockerfile
touch "$ROOT"/spark/requirements.txt
touch "$ROOT"/spark/conf/spark-defaults.conf

touch "$ROOT"/spark/jobs/bronze_to_silver.py
touch "$ROOT"/spark/jobs/product_matching.py
touch "$ROOT"/spark/jobs/silver_to_gold.py

# ============================================================
# dbt
# ============================================================

touch "$ROOT"/dbt/dbt_project.yml
touch "$ROOT"/dbt/profiles.yml

touch "$ROOT"/dbt/models/sources.yml

touch "$ROOT"/dbt/models/staging/stg_declarations.sql
touch "$ROOT"/dbt/models/staging/stg_market_prices.sql
touch "$ROOT"/dbt/models/staging/stg_product_matches.sql

touch "$ROOT"/dbt/models/marts/fct_declarations_valuation.sql
touch "$ROOT"/dbt/models/marts/mart_risk_by_hs_code.sql
touch "$ROOT"/dbt/models/marts/mart_daily_kpis.sql
touch "$ROOT"/dbt/models/marts/schema.yml

touch "$ROOT"/dbt/tests/assert_positive_declared_value.sql

# ============================================================
# Infrastructure
# ============================================================

touch "$ROOT"/infrastructure/minio/init-buckets.sh

touch "$ROOT"/infrastructure/iceberg/README.md

touch "$ROOT"/infrastructure/trino/catalog/iceberg.properties

# ============================================================
# Grafana
# ============================================================

touch "$ROOT"/monitoring/grafana/provisioning/datasources/trino.yml
touch "$ROOT"/monitoring/grafana/provisioning/dashboards/dashboards.yml
touch "$ROOT"/monitoring/grafana/dashboards/adii_overview.json

# ============================================================
# Make scripts executable
# ============================================================

chmod +x "$ROOT"/airflow/scripts/entrypoint.sh
chmod +x "$ROOT"/infrastructure/minio/init-buckets.sh

# ============================================================
# .gitignore
# ============================================================

cat > "$ROOT"/.gitignore <<'EOF'
# Environment
.env

# Python
__pycache__/
*.py[cod]
.venv/
venv/

# Local data
data/*
!data/.gitkeep

# Jupyter
.ipynb_checkpoints/

# Spark
spark-warehouse/
metastore_db/

# dbt
dbt_packages/
target/
logs/

# Airflow
airflow/logs/

# OS
.DS_Store
Thumbs.db
EOF

# ============================================================
# Environment example
# ============================================================

cat > "$ROOT"/.env.example <<'EOF'
# MinIO
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin

MINIO_ENDPOINT=http://minio:9000
MINIO_BUCKET=datalake

# Airflow
AIRFLOW_UID=50000

# Trino
TRINO_HOST=trino
TRINO_PORT=8080

# Iceberg
ICEBERG_CATALOG=rest
ICEBERG_REST_URI=http://iceberg-rest:8181

# S3
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_REGION=us-east-1
EOF

# ============================================================
# README
# ============================================================

cat > "$ROOT"/README.md <<'EOF'
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
EOF

echo ""
echo "=========================================="
echo " DataPlatformADII structure created!"
echo "=========================================="
echo ""
echo "Project:"
echo "$ROOT/"
echo ""
echo "Existing files were NOT overwritten."
echo "The script is idempotent."
echo ""
echo "Run:"
echo "  tree $ROOT"
echo ""