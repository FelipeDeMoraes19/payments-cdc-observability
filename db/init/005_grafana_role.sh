#!/bin/bash
set -euo pipefail

if [ -z "${GRAFANA_DB_USER:-}" ] || [ -z "${GRAFANA_DB_PASSWORD:-}" ]; then
    echo "GRAFANA_DB_USER and GRAFANA_DB_PASSWORD must be set; copy .env.example to .env" >&2
    exit 1
fi

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<EOSQL
CREATE ROLE "$GRAFANA_DB_USER" WITH LOGIN PASSWORD '$GRAFANA_DB_PASSWORD';
GRANT CONNECT ON DATABASE "$POSTGRES_DB" TO "$GRAFANA_DB_USER";
EOSQL
