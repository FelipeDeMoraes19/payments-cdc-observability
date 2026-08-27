import re
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.e2e

ROOT = Path(__file__).resolve().parents[1]
STEP_TIMEOUT_SECONDS = 900
MARKER = "===SECTION==="

PROBE_DAGS = """
import importlib.util
for name in ('fx_daily', 'bronze_to_gold'):
    path = '/opt/project/orchestration/airflow/dags/%s.py' % name
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    print('%s catchup=%s' % (name, module.dag.catchup))
"""

SECTIONS = (
    ("dags_list", "airflow dags list"),
    ("catchup", "python - <<'PROBE'\n{}\nPROBE".format(PROBE_DAGS)),
    ("render_august", "airflow tasks render fx_daily extract_rates 2026-08-24"),
    ("render_july", "airflow tasks render fx_daily extract_rates 2026-07-09"),
    (
        "backfill_week",
        "airflow backfill create --dag-id fx_daily "
        "--from-date 2026-08-17 --to-date 2026-08-24 --dry-run",
    ),
    (
        "backfill_pair",
        "airflow backfill create --dag-id fx_daily "
        "--from-date 2026-08-17 --to-date 2026-08-19 --dry-run",
    ),
)


@pytest.fixture(scope="module")
def cli():
    """Every Airflow CLI call the wiring tests need, in one container.

    Measured on this machine: starting the container costs about a second, but
    bootstrapping the Airflow CLI costs about eleven. Paying that once for six
    commands rather than once per command is where the time actually is; it is
    not in container creation, which is what the obvious guess would have been.
    """
    script = "airflow dags reserialize\n" + "\n".join(
        'echo "{} {}"\n{}'.format(MARKER, name, command) for name, command in SECTIONS
    )
    completed = subprocess.run(
        [
            "docker", "compose", "--profile", "orchestration", "run", "--rm",
            "airflow-init", "bash", "-c", script,
        ],
        cwd=str(ROOT), capture_output=True, timeout=STEP_TIMEOUT_SECONDS,
    )
    output = completed.stdout.decode("utf-8", "replace")
    output += completed.stderr.decode("utf-8", "replace")
    assert completed.returncode == 0, output

    sections = {}
    current = None
    for line in output.splitlines():
        if MARKER in line:
            current = line.split(MARKER)[1].strip()
            sections[current] = []
        elif current:
            sections[current].append(line)
    missing = [name for name, _ in SECTIONS if name not in sections]
    assert not missing, "the container produced no output for {}:\n{}".format(missing, output)
    return {name: "\n".join(lines) for name, lines in sections.items()}


def test_both_dags_exist_and_neither_catches_up(cli):
    assert "fx_daily" in cli["dags_list"]
    assert "bronze_to_gold" in cli["dags_list"]
    assert "fx_daily catchup=False" in cli["catchup"]
    assert "bronze_to_gold catchup=False" in cli["catchup"]


def test_the_extractor_receives_the_date_of_its_own_interval(cli):
    assert "'FX_START_DATE': '2026-08-24'" in cli["render_august"]
    assert "'FX_END_DATE': '2026-08-24'" in cli["render_august"]
    assert "'FX_START_DATE': '2026-07-09'" in cli["render_july"], (
        "the rendered date did not follow the interval, so it may be hardcoded"
    )


def proposed_runs(output: str) -> list:
    """The logical dates in the table, not every date printed anywhere.

    An earlier version searched the whole output, which also contains the echoed
    --to-date parameter. It asserted seven runs and passed while the command
    actually proposed six, because the seventh date appeared only in the
    parameter listing. Reading the table alone is what makes the assertion mean
    what it says.
    """
    marker = "Runs to be attempted"
    assert marker in output, "the dry run printed no table of runs: {}".format(output)
    table = output[output.index(marker):]
    return sorted(set(re.findall(r"\d{4}-\d{2}-\d{2}(?= \d{2}:\d{2})", table)))


def test_backfill_proposes_one_run_for_each_scheduled_time_in_the_window(cli):
    week = proposed_runs(cli["backfill_week"])
    assert week == [
        "2026-08-17", "2026-08-18", "2026-08-19", "2026-08-20",
        "2026-08-21", "2026-08-22", "2026-08-23",
    ], "backfill proposed {}".format(week)

    pair = proposed_runs(cli["backfill_pair"])
    assert pair == ["2026-08-17", "2026-08-18"], (
        "a narrow window proposed {} instead of its own two days".format(pair)
    )
