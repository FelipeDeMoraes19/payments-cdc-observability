.PHONY: env alerts up down reset seed fx test-chaos runner-build silver runner-shell gold docs airflow airflow-down backfill-fx test test-e2e test-all slots

env:
	python scripts/bootstrap_env.py

alerts:
	MSYS_NO_PATHCONV=1 docker compose --profile jobs run --rm terraform init -input=false
	MSYS_NO_PATHCONV=1 docker compose --profile jobs run --rm terraform apply -auto-approve -input=false

up: env
	docker compose up -d

down:
	docker compose down

reset: env
	docker compose down -v
	docker compose up -d

seed:
	python -m generator.synthetic_load


fx:
	python -m ingestion.batch.bcb_fx

runner-build:
	docker compose --profile jobs build runner

silver:
	MSYS_NO_PATHCONV=1 docker compose --profile jobs run --rm runner bash -c "spark-submit --master 'local[*]' transform/spark/bronze_to_silver.py"

runner-shell:
	MSYS_NO_PATHCONV=1 docker compose --profile jobs run --rm runner bash

gold:
	cd transform/dbt && DBT_PROFILES_DIR=. dbt build

docs:
	cd transform/dbt && DBT_PROFILES_DIR=. dbt docs generate
	cp transform/dbt/target/index.html docs/index.html
	cp transform/dbt/target/manifest.json docs/manifest.json
	cp transform/dbt/target/catalog.json docs/catalog.json

airflow:
	docker compose --profile orchestration up -d

airflow-down:
	docker compose --profile orchestration down

backfill-fx:
	MSYS_NO_PATHCONV=1 docker compose --profile orchestration run --rm airflow-init bash -c "airflow backfill create --dag-id fx_daily --from-date $(FROM) --to-date $(TO)"

test:
	python -m pytest -m 'not e2e and not chaos' -v

test-e2e:
	docker compose stop cdc generator
	-python -m pytest -m e2e -v
	docker compose start cdc generator

test-chaos:
	python -m pytest -m chaos -v

test-all:
	docker compose stop cdc generator
	-python -m pytest -m 'not chaos' -v
	docker compose start cdc generator

slots:
	docker compose exec -T postgres psql -U $${POSTGRES_USER:-payments} -d $${POSTGRES_DB:-payments} -c "SELECT slot_name, active, restart_lsn, confirmed_flush_lsn, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal FROM pg_replication_slots ORDER BY slot_name;"
