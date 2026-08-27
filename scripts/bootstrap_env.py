import re
import secrets
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / ".env.example"
TARGET = ROOT / ".env"
PLACEHOLDER = re.compile(r"^generate-your-own-with-python-secrets-token-hex-(\d+)$")


def resolve(value: str) -> str:
    match = PLACEHOLDER.match(value.strip())
    if match:
        return secrets.token_hex(int(match.group(1)))
    return value


def keys_of(text: str) -> set:
    return {
        line.split("=", 1)[0].strip()
        for line in text.splitlines()
        if "=" in line and not line.lstrip().startswith("#")
    }


def main() -> int:
    if not EXAMPLE.exists():
        print("missing .env.example, cannot bootstrap", file=sys.stderr)
        return 1
    example = EXAMPLE.read_text(encoding="utf-8")

    if not TARGET.exists():
        lines, generated = [], 0
        for line in example.splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                key, value = line.split("=", 1)
                resolved = resolve(value)
                generated += resolved != value
                line = "{}={}".format(key, resolved)
            lines.append(line)
        TARGET.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")
        print("wrote .env with {} generated secret(s)".format(generated), file=sys.stderr)
        return 0

    current = TARGET.read_text(encoding="utf-8")
    missing = keys_of(example) - keys_of(current)
    if not missing:
        print(".env is complete, leaving it alone", file=sys.stderr)
        return 0

    added = []
    for line in example.splitlines():
        if "=" not in line or line.lstrip().startswith("#"):
            continue
        key, value = line.split("=", 1)
        if key.strip() in missing:
            added.append("{}={}".format(key, resolve(value)))
    TARGET.write_text(
        current.rstrip("\n") + "\n" + "\n".join(added) + "\n", encoding="utf-8", newline="\n"
    )
    print(
        "added {} key(s) missing from .env: {}".format(
            len(added), ", ".join(sorted(missing))
        ),
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
