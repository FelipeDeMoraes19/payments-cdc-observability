import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
README = ROOT / "README.md"
MAKEFILE = ROOT / "Makefile"

FILE_LIKE = re.compile(r"^[\w./-]+\.(py|sql|tf|yml|yaml|sh)$")
MAKE_TARGET = re.compile(r"^make ([\w-]+)$")
CLAIMS_PROOF = re.compile(r"\*\*Yes", re.IGNORECASE)


def table_rows() -> list:
    """The failure mode table, as a list of {header: cell} rows."""
    lines = README.read_text(encoding="utf-8").splitlines()
    start = next(i for i, line in enumerate(lines) if line.startswith("| Failure mode |"))
    headers = [cell.strip() for cell in lines[start].strip("|").split("|")]
    rows = []
    for line in lines[start + 2 :]:
        if not line.startswith("|"):
            break
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if len(cells) == len(headers):
            rows.append(dict(zip(headers, cells)))
    return rows


def cited_tokens(row: dict) -> list:
    return re.findall(r"`([^`]+)`", " ".join(row.values()))


def make_targets() -> set:
    return set(
        re.findall(r"^([\w-]+):", MAKEFILE.read_text(encoding="utf-8"), re.MULTILINE)
    )


def test_the_table_is_not_empty():
    rows = table_rows()
    assert len(rows) > 5, "only {} rows parsed, so the checks below prove little".format(
        len(rows)
    )


def test_every_file_the_table_cites_exists():
    """The table is the centre of this repository, and it can lie.

    It once named a not_null test on amount_brl as the detector for a missing
    exchange rate. That test was never written. A table that promises a detector
    nobody built is precisely the failure this project is about — an alert nobody
    knows is blind — sitting in the document that explains the idea.
    """
    missing = []
    for row in table_rows():
        for token in cited_tokens(row):
            if FILE_LIKE.match(token) and not (ROOT / token).exists():
                missing.append((row["Failure mode"][:40], token))
    assert not missing, "the table cites files that do not exist: {}".format(missing)


def test_every_make_target_the_table_cites_exists():
    targets = make_targets()
    assert targets, "no targets parsed from the Makefile"
    missing = []
    for row in table_rows():
        for token in cited_tokens(row):
            match = MAKE_TARGET.match(token)
            if match and match.group(1) not in targets:
                missing.append((row["Failure mode"][:40], token))
    assert not missing, "the table cites make targets that do not exist: {}".format(missing)


def test_every_row_claiming_proof_cites_something_checkable():
    """A row may not claim to be proven on prose alone.

    Naming a mechanism in English is a promise; naming a file or a target is a
    thing this test can verify. Rows still marked planned are exempt, because
    they are not claiming anything yet.
    """
    unverifiable = []
    for row in table_rows():
        verdict = row["Caught?"]
        if not CLAIMS_PROOF.search(verdict):
            continue
        checkable = [
            token
            for token in cited_tokens(row)
            if FILE_LIKE.match(token) or MAKE_TARGET.match(token)
        ]
        adr = re.search(r"ADR \d{4}", verdict)
        if not checkable and not adr:
            unverifiable.append(row["Failure mode"][:50])
    assert not unverifiable, (
        "these rows claim to be proven but cite nothing a test can check: {}".format(
            unverifiable
        )
    )
