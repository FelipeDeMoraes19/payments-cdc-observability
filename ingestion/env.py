import os
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def load_env_file(path=None) -> bool:
    target = Path(path) if path else REPOSITORY_ROOT / ".env"
    if not target.exists():
        return False
    for line in target.read_text(encoding="utf-8").splitlines():
        entry = line.strip()
        if not entry or entry.startswith("#") or "=" not in entry:
            continue
        key, value = entry.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())
    return True


def required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            "{} is not set; copy .env.example to .env before running".format(name)
        )
    return value
