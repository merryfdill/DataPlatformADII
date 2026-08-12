#!/bin/sh
# One-shot job: create the datalake bucket and bronze/silver/gold prefixes.
# Run via the minio-init service (mc image) after MinIO is healthy.
set -e

mc alias set local "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"

mc mb --ignore-existing "local/${MINIO_BUCKET}"
mc mb --ignore-existing "local/${MINIO_BUCKET}/bronze"
mc mb --ignore-existing "local/${MINIO_BUCKET}/silver"
mc mb --ignore-existing "local/${MINIO_BUCKET}/gold"

echo "MinIO bucket '${MINIO_BUCKET}' ready with bronze/silver/gold prefixes."
