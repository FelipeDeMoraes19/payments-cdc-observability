#!/bin/bash
set -euo pipefail

if [ -z "${CDC_USER:-}" ] || [ -z "${CDC_PASSWORD:-}" ]; then
    echo "CDC_USER and CDC_PASSWORD must be set; copy .env.example to .env" >&2
    exit 1
fi

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<EOSQL
CREATE ROLE "$CDC_USER" WITH LOGIN REPLICATION PASSWORD '$CDC_PASSWORD';
GRANT CONNECT ON DATABASE "$POSTGRES_DB" TO "$CDC_USER";
GRANT USAGE ON SCHEMA public TO "$CDC_USER";
GRANT SELECT ON ALL TABLES IN SCHEMA public TO "$CDC_USER";
EOSQL
