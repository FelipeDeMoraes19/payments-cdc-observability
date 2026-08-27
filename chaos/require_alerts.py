import base64
import json
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def env(name: str, fallback: str) -> str:
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        if line.startswith(name + "="):
            return line.split("=", 1)[1]
    return fallback


def main() -> int:
    url = "http://localhost:{}/api/v1/provisioning/alert-rules".format(
        env("GRAFANA_PORT", "3000")
    )
    request = urllib.request.Request(url)
    credentials = "{}:{}".format(
        env("GRAFANA_ADMIN_USER", "admin"), env("GRAFANA_ADMIN_PASSWORD", "")
    )
    request.add_header(
        "Authorization", "Basic " + base64.b64encode(credentials.encode()).decode()
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            rules = json.loads(response.read().decode("utf-8"))
    except Exception as error:
        print(
            "cannot reach Grafana at {}: {}\n"
            "this scenario is detected by an alert, so it proves nothing without one.\n"
            "run: make up && make alerts".format(url, error),
            file=sys.stderr,
        )
        return 1
    if not rules:
        print(
            "Grafana is up but has no alert rules, so this scenario would inject a "
            "failure nobody is watching for.\nrun: make alerts",
            file=sys.stderr,
        )
        return 1
    print("{} alert rule(s) provisioned".format(len(rules)), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
