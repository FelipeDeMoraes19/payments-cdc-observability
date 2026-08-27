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


def main() -> int:
    if TARGET.exists():
        print(".env already exists, leaving it alone", file=sys.stderr)
        return 0
    if not EXAMPLE.exists():
        print("missing .env.example, cannot bootstrap", file=sys.stderr)
        return 1
    generated = 0
    lines = []
    for line in EXAMPLE.read_text(encoding="utf-8").splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            key, value = line.split("=", 1)
            resolved = resolve(value)
            generated += resolved != value
            line = "{}={}".format(key, resolved)
        lines.append(line)
    TARGET.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")
    print(
        "wrote .env with {} generated secret(s); it is git ignored".format(generated),
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
