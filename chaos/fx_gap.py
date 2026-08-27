import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FX = ROOT / "data" / "bronze" / "fx"


def main() -> int:
    months = sorted(FX.glob("*/month=*"))
    if not months:
        print(
            "no exchange rate months in {}; run make fx first".format(FX), file=sys.stderr
        )
        return 1
    victim = months[-1]
    shutil.rmtree(victim)
    print(
        "removed {}\nrebuild gold and amount_brl goes null for payments on those days, "
        "which the not_null test on amount_brl is there to catch".format(
            victim.relative_to(ROOT)
        ),
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
