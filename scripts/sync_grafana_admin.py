import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def env(name: str) -> str:
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        if line.startswith(name + "="):
            return line.split("=", 1)[1].strip()
    raise SystemExit("{} is not in .env; run make up first".format(name))


def main() -> int:
    """Force Grafana's admin password to match .env.

    GF_SECURITY_ADMIN_PASSWORD is only read when Grafana initialises its own
    database. After that the volume holds whatever was set then, and every later
    change to .env is ignored in silence until someone gets a 401 from a command
    that gives no hint why. Regenerating .env, rotating the password, or cloning
    the repository beside an existing volume all land there.

    This runs before every terraform apply, costs a second, and is idempotent.
    """
    password = env("GRAFANA_ADMIN_PASSWORD")
    completed = subprocess.run(
        [
            "docker", "compose", "exec", "-T", "grafana", "sh", "-c",
            "cd /usr/share/grafana && grafana cli admin reset-admin-password "
            "'{}'".format(password),
        ],
        cwd=str(ROOT), capture_output=True, timeout=120,
    )
    output = completed.stdout.decode("utf-8", "replace")
    output += completed.stderr.decode("utf-8", "replace")
    if completed.returncode != 0:
        print(
            "could not set Grafana's admin password. is the stack up?\n" + output,
            file=sys.stderr,
        )
        return 1
    print("Grafana admin password now matches .env", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
