import base64
import sys
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def env(name: str, fallback: str = "") -> str:
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        if line.startswith(name + "="):
            return line.split("=", 1)[1].strip()
    return fallback


def main() -> int:
    """Fail with an explanation instead of a bare 401 from Terraform.

    Grafana reads GF_SECURITY_ADMIN_PASSWORD only when it initialises its own
    database. After that the volume holds whatever was set then, and a later
    change to .env is ignored in silence. The CLI's reset-admin-password reports
    success against a running server and does not take effect, so it is not a
    way out. Recreating the volume is.
    """
    user = env("GRAFANA_ADMIN_USER", "admin")
    password = env("GRAFANA_ADMIN_PASSWORD")
    url = "http://localhost:{}/api/org".format(env("GRAFANA_PORT", "3000"))
    request = urllib.request.Request(url)
    request.add_header(
        "Authorization",
        "Basic " + base64.b64encode("{}:{}".format(user, password).encode()).decode(),
    )
    try:
        with urllib.request.urlopen(request, timeout=15):
            return 0
    except urllib.error.HTTPError as error:
        if error.code != 401:
            raise
    except Exception as error:
        print(
            "cannot reach Grafana at {}: {}\nis the stack up? try make up".format(
                url, error
            ),
            file=sys.stderr,
        )
        return 1
    print(
        "Grafana rejected the admin password in .env.\n"
        "\n"
        "Grafana only reads GF_SECURITY_ADMIN_PASSWORD when it first creates its\n"
        "database. Its volume outlived a change to .env, so it still holds an older\n"
        "password and no amount of editing .env will change that.\n"
        "\n"
        "  docker compose down -v && make up && make alerts\n"
        "\n"
        "That recreates the volume. It also destroys the database, which is synthetic\n"
        "and rebuilds itself.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
