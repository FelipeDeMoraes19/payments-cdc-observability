import os
import subprocess
import sys
from pathlib import Path

import psycopg2
import pytest

from ingestion.cdc import bronze
from ingestion.env import load_env_file

ROOT = Path(__file__).resolve().parents[1]
CONSUMER_TIMEOUT_SECONDS = 60


def source_params() -> dict:
    return dict(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5434"),
        dbname=os.environ.get("POSTGRES_DB", "payments"),
        user=os.environ.get("POSTGRES_USER", "payments"),
        password=os.environ.get("POSTGRES_PASSWORD", "payments"),
    )


@pytest.fixture
def postgres():
    load_env_file()
    connection = psycopg2.connect(**source_params())
    connection.autocommit = True
    yield connection
    connection.close()


@pytest.fixture
def reset_source(postgres):
    def reset(slot: str) -> None:
        with postgres.cursor() as cursor:
            cursor.execute(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                "WHERE slot_name = %s",
                (slot,),
            )
            cursor.execute("DELETE FROM payments")
            cursor.execute("DELETE FROM customers")
            cursor.execute("DELETE FROM merchants")
            cursor.execute(
                "SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (slot,)
            )

    return reset


@pytest.fixture
def drop_slot(postgres):
    def drop(slot: str) -> None:
        with postgres.cursor() as cursor:
            cursor.execute(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                "WHERE slot_name = %s",
                (slot,),
            )

    return drop


@pytest.fixture
def consumer_env():
    def build(slot: str, bronze_root, **overrides) -> dict:
        settings = {"CDC_IDLE_TIMEOUT": "4", "CDC_BATCH_MAX_SECONDS": "1"}
        settings.update(overrides)
        environment = dict(os.environ)
        environment["PYTHONPATH"] = str(ROOT)
        environment["CDC_SLOT"] = slot
        environment["CDC_BRONZE_ROOT"] = str(bronze_root)
        environment.update({name: str(value) for name, value in settings.items()})
        return environment

    return build


@pytest.fixture
def run_consumer(consumer_env):
    def run(slot: str, bronze_root, **overrides) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "ingestion.cdc.consumer"],
            cwd=str(ROOT),
            env=consumer_env(slot, bronze_root, **overrides),
            capture_output=True,
            timeout=CONSUMER_TIMEOUT_SECONDS,
        )

    return run


@pytest.fixture
def read_bronze():
    return bronze.read
