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
        "removed {}\nrebuild gold with make gold: fx_covers_the_payments fails, "
        "because the quotes no longer reach the days payments were made on".format(
            victim.relative_to(ROOT)
        ),
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
