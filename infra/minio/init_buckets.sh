#!/usr/bin/env sh
set -e

mc alias set local http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"
mc mb --ignore-existing "local/${DELTA_LAKE_BUCKET}"
echo "bucket '${DELTA_LAKE_BUCKET}' ready"
