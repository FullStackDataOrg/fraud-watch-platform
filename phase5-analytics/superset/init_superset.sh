#!/bin/bash
set -e

superset db upgrade
superset fab create-admin \
    --username "${SUPERSET_ADMIN_USER:-admin}" \
    --firstname Admin \
    --lastname User \
    --email admin@fraud-platform.local \
    --password "${SUPERSET_ADMIN_PASSWORD:-admin123}" 2>/dev/null || true

superset init

# Register Trino connection
superset set_database_uri \
    --database_name "Trino (Delta Lake)" \
    --uri "trino://admin@trino:8080/delta" 2>/dev/null || true

echo "Superset initialised"
exec superset run -h 0.0.0.0 -p 8088 --with-threads
