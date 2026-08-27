import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.e2e

ROOT = Path(__file__).resolve().parents[1]
STEP_TIMEOUT_SECONDS = 600


def airflow(command: str):
    completed = subprocess.run(
        [
            "docker", "compose", "--profile", "orchestration", "run", "--rm",
            "airflow-init", "bash", "-c", command,
        ],
        cwd=str(ROOT), capture_output=True, timeout=STEP_TIMEOUT_SECONDS,
    )
    output = completed.stdout.decode("utf-8", "replace") + completed.stderr.decode("utf-8", "replace")
    return completed.returncode, output


@pytest.fixture(scope="module", autouse=True)
def registered_dags():
    returncode, output = airflow("airflow dags reserialize")
    assert returncode == 0, output


def test_both_dags_exist_and_neither_catches_up():
    returncode, output = airflow("airflow dags list")
    assert returncode == 0, output
    assert "fx_daily" in output
    assert "cdc_to_gold" in output

    returncode, details = airflow(
        "python - <<'PY'\n"
        "import importlib.util\n"
        "for name in ('fx_daily', 'cdc_to_gold'):\n"
        "    path = '/opt/project/orchestration/airflow/dags/%s.py' % name\n"
        "    spec = importlib.util.spec_from_file_location(name, path)\n"
        "    module = importlib.util.module_from_spec(spec)\n"
        "    spec.loader.exec_module(module)\n"
        "    print('%s catchup=%s' % (name, module.dag.catchup))\n"
        "PY"
    )
    assert returncode == 0, details
    assert "fx_daily catchup=False" in details, details
    assert "cdc_to_gold catchup=False" in details, details


def test_the_extractor_receives_the_date_of_its_own_interval():
    returncode, output = airflow("airflow tasks render fx_daily extract_rates 2026-08-24")
    assert returncode == 0, output
    assert "'FX_START_DATE': '2026-08-24'" in output, output
    assert "'FX_END_DATE': '2026-08-24'" in output, output

    returncode, other = airflow("airflow tasks render fx_daily extract_rates 2026-07-09")
    assert returncode == 0, other
    assert "'FX_START_DATE': '2026-07-09'" in other, (
        "the rendered date did not follow the interval, so it may be hardcoded"
    )


def test_backfill_proposes_one_run_for_each_day_in_the_window():
    returncode, output = airflow(
        "airflow backfill create --dag-id fx_daily "
        "--from-date 2026-08-17 --to-date 2026-08-23 --dry-run"
    )
    assert returncode == 0, output
    proposed = {
        "2026-08-{:02d}".format(day) for day in range(17, 24) if "2026-08-{:02d}".format(day) in output
    }
    assert proposed == {
        "2026-08-17", "2026-08-18", "2026-08-19", "2026-08-20",
        "2026-08-21", "2026-08-22", "2026-08-23",
    }, "backfill did not propose exactly the seven days of the window:\n{}".format(output)

    returncode, narrower = airflow(
        "airflow backfill create --dag-id fx_daily "
        "--from-date 2026-08-17 --to-date 2026-08-18 --dry-run"
    )
    assert returncode == 0, narrower
    assert "2026-08-19" not in narrower, (
        "a two day window proposed a day outside it, so the window is not respected"
    )
