.PHONY: up down reset seed cdc fx spark-build spark-shell test slots

up:
	docker compose up -d

down:
	docker compose down

reset:
	docker compose down -v
	docker compose up -d

seed:
	python -m generator.synthetic_load

cdc:
	python -m ingestion.cdc.consumer

fx:
	python -m ingestion.batch.bcb_fx

spark-build:
	docker compose --profile jobs build spark

spark-shell:
	MSYS_NO_PATHCONV=1 docker compose --profile jobs run --rm spark /opt/spark/bin/pyspark --master 'local[*]'

test:
	python -m pytest tests -v

slots:
	docker compose exec -T postgres psql -U $${POSTGRES_USER:-payments} -d $${POSTGRES_DB:-payments} -c "SELECT slot_name, active, restart_lsn, confirmed_flush_lsn, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal FROM pg_replication_slots ORDER BY slot_name;"
