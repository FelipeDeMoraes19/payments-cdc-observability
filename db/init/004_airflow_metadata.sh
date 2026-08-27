#!/bin/bash
set -euo pipefail

if [ -z "${AIRFLOW_DB_USER:-}" ] || [ -z "${AIRFLOW_DB_PASSWORD:-}" ]; then
    echo "AIRFLOW_DB_USER and AIRFLOW_DB_PASSWORD must be set; copy .env.example to .env" >&2
    exit 1
fi

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<EOSQL
CREATE ROLE "$AIRFLOW_DB_USER" WITH LOGIN PASSWORD '$AIRFLOW_DB_PASSWORD';
CREATE DATABASE "$AIRFLOW_DB_NAME" OWNER "$AIRFLOW_DB_USER";
EOSQL
