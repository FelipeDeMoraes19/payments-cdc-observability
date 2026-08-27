import subprocess
import time
from pathlib import Path

import pytest

pytestmark = pytest.mark.chaos

ROOT = Path(__file__).resolve().parents[1]
CONTAINER = "payments-oltp"
FLIP_TIMEOUT_SECONDS = 90


def health() -> str:
    completed = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Health.Status}}", CONTAINER],
        cwd=str(ROOT), capture_output=True, timeout=30,
    )
    return completed.stdout.decode("utf-8", "replace").strip()


def wait_for(state: str) -> bool:
    deadline = time.monotonic() + FLIP_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if health() == state:
            return True
        time.sleep(3)
    return False


def configured_password() -> str:
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        if line.startswith("GRAFANA_DB_PASSWORD="):
            return line.split("=", 1)[1]
    raise AssertionError("GRAFANA_DB_PASSWORD is not in .env")


@pytest.fixture
def grafana_role(postgres):
    """Restores the role with the password the rest of the stack expects.

    An earlier version invented one. The role came back, the healthcheck went
    green, and Grafana's data source quietly stopped authenticating until another
    test tripped over it. A restore that leaves the system subtly broken is worse
    than no restore, because it looks finished.
    """
    password = configured_password()
    yield
    with postgres.cursor() as cursor:
        cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = 'grafana'")
        if cursor.fetchone() is None:
            cursor.execute("CREATE ROLE grafana WITH LOGIN PASSWORD %s", (password,))
            cursor.execute("GRANT CONNECT ON DATABASE payments TO grafana")
    assert wait_for("healthy"), "the database did not recover after the role was restored"


def test_an_incomplete_init_makes_the_database_unhealthy(postgres, grafana_role):
    """Passing a healthcheck and being ready for use are different things.

    An init script that fails leaves the database missing whatever it was
    supposed to create, while the container still accepts connections. The
    original healthcheck asked "do you accept connections", so it answered yes
    and the missing role was found much later, by hand.

    It now asks "did the init scripts finish", by checking the roles and the
    publication they create. Dropping one of those artifacts stands in for the
    script that failed to create it.
    """
    assert health() == "healthy", "the database was not healthy before the injection"

    with postgres.cursor() as cursor:
        cursor.execute("REVOKE CONNECT ON DATABASE payments FROM grafana")
        cursor.execute("DROP ROLE grafana")

    assert wait_for("unhealthy"), (
        "the database still reports healthy while an init artifact is missing, "
        "so the healthcheck is back to answering the easy question"
    )
