import json
import os
import re
import subprocess
import urllib.parse
import urllib.request
from pathlib import Path

import psycopg2
import pytest

pytestmark = pytest.mark.chaos

ROOT = Path(__file__).resolve().parents[1]
ALERTS_TF = ROOT / "observability" / "terraform" / "alerts.tf"
GRAFANA_API = "http://localhost:{}/api/v1/provisioning/alert-rules"
PROMETHEUS_API = "http://localhost:{}/api/v1/query"

KNOWN_BLIND = "Archive ingestion stalled (KNOWN BLIND, see ADR 0006)"


def env(name: str, fallback: str) -> str:
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        if line.startswith(name + "="):
            return line.split("=", 1)[1]
    return fallback


def declared_rule_count() -> int:
    return len(re.findall(r"^  rule \{", ALERTS_TF.read_text(encoding="utf-8"), re.MULTILINE))


def provisioned_rules() -> list:
    url = GRAFANA_API.format(env("GRAFANA_PORT", "3000"))
    request = urllib.request.Request(url)
    credentials = "{}:{}".format(env("GRAFANA_ADMIN_USER", "admin"), env("GRAFANA_ADMIN_PASSWORD", ""))
    import base64

    request.add_header(
        "Authorization", "Basic " + base64.b64encode(credentials.encode()).decode()
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def prometheus_returns_series(expr: str) -> bool:
    url = PROMETHEUS_API.format(env("PROMETHEUS_PORT", "9090"))
    url += "?" + urllib.parse.urlencode({"query": expr})
    with urllib.request.urlopen(url, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return bool(payload["data"]["result"])


def postgres_returns_rows(sql: str) -> bool:
    connection = psycopg2.connect(
        host="localhost",
        port=env("POSTGRES_PORT", "5434"),
        dbname=env("POSTGRES_DB", "payments"),
        user=env("GRAFANA_DB_USER", "grafana"),
        password=env("GRAFANA_DB_PASSWORD", ""),
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            return bool(cursor.fetchall())
    finally:
        connection.close()


def query_of(rule: dict) -> tuple:
    for node in rule.get("data", []):
        model = node.get("model", {})
        if "expr" in model:
            return "prometheus", model["expr"]
        if "rawSql" in model:
            return "postgres", model["rawSql"]
    return "none", ""


def test_every_alert_is_capable_of_firing():
    """An alert whose query never returns a series can never fire.

    The count is asserted first and that is the part that matters. Alert rules
    are provisioned by make alerts, not by docker compose up, so on a stack where
    nobody ran it this guard would iterate over an empty list, find no violation
    and pass. A vacuity guard that passes because it had nothing to inspect is
    the exact defect this repository exists to point at, sitting inside the piece
    built to point at it.

    What it covers, and the limit belongs in writing: it detects blindness by
    empty series, not blindness by unreachable threshold. An alert with a healthy
    query and a threshold of 10^12 would pass this and be just as blind.
    """
    declared = declared_rule_count()
    assert declared > 0, "no rule blocks found in alerts.tf, so the guard has nothing to check"

    rules = provisioned_rules()
    assert len(rules) == declared, (
        "alerts.tf declares {} rules and Grafana has {}; run make alerts".format(
            declared, len(rules)
        )
    )

    titles = {rule["title"] for rule in rules}
    assert KNOWN_BLIND in titles, (
        "the known blind alert is missing, so the exception below would silently "
        "cover nothing"
    )

    blind = []
    for rule in rules:
        if rule["title"] == KNOWN_BLIND:
            continue
        kind, query = query_of(rule)
        assert kind != "none", "rule {!r} has no query to evaluate".format(rule["title"])
        answered = (
            prometheus_returns_series(query)
            if kind == "prometheus"
            else postgres_returns_rows(query)
        )
        if not answered:
            blind.append((rule["title"], kind, query))

    assert not blind, (
        "these alerts return no series and therefore cannot fire: {}".format(blind)
    )


def test_the_known_blind_alert_is_blind_for_both_declared_reasons():
    """The exception is asserted, not trusted.

    ADR 0006 says the alert is blind for two reasons that are individually
    defensible: a label value nothing carries, and no data treated as normal. If
    either stopped being true the alert might start firing, and the exception in
    the guard above would be covering an alert that no longer needs it.
    """
    rules = {rule["title"]: rule for rule in provisioned_rules()}
    assert KNOWN_BLIND in rules
    blind = rules[KNOWN_BLIND]

    kind, query = query_of(blind)
    assert kind == "prometheus"
    assert not prometheus_returns_series(query), (
        "the blind alert's query returned a series, so its first mechanism is gone"
    )
    assert blind["noDataState"] == "OK", (
        "the blind alert no longer treats no data as normal, so its second "
        "mechanism is gone and it may fire"
    )
