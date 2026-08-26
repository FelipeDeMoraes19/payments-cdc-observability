import calendar
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from pydantic import ValidationError

from contracts.fx import FX_SERIES, FxSeries, SgsObservation
from contracts.validation import ContractViolation
from ingestion.env import load_env_file

SGS_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{}/dados"
SOURCE = "bcb-sgs"
FILE_NAME = "observations.parquet"
STAGING_NAME = FILE_NAME + ".tmp"

SCHEMA = pa.schema(
    [
        pa.field("source", pa.string(), nullable=False),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("series_code", pa.int32(), nullable=False),
        pa.field("currency", pa.string(), nullable=False),
        pa.field("quote_date", pa.date32(), nullable=False),
        pa.field("rate_brl", pa.decimal128(18, 8), nullable=False),
    ]
)


def bronze_root() -> Path:
    return Path(os.environ.get("FX_BRONZE_ROOT", "data/bronze/fx"))


def default_start() -> date:
    return date.fromisoformat(os.environ.get("FX_DEFAULT_START", "2026-01-01"))


def request_timeout() -> float:
    return float(os.environ.get("FX_TIMEOUT_SECONDS", "30"))


def _brazilian(value: date) -> str:
    return value.strftime("%d/%m/%Y")


def _month_directory(root: Path, series: FxSeries, month: date) -> Path:
    return root / series.currency / "month={:%Y-%m}".format(month)


def month_windows(start: date, end: date) -> list:
    windows = []
    cursor = start.replace(day=1)
    while cursor <= end:
        last_day = calendar.monthrange(cursor.year, cursor.month)[1]
        month_end = min(cursor.replace(day=last_day), end)
        windows.append((cursor, month_end))
        cursor = cursor.replace(day=last_day) + timedelta(days=1)
    return windows


def watermark(root: Path, series: FxSeries):
    directory = root / series.currency
    if not directory.exists():
        return None
    latest = None
    for path in directory.rglob(FILE_NAME):
        column = pq.read_table(path, columns=["quote_date"]).column("quote_date")
        for value in column.to_pylist():
            if latest is None or value > latest:
                latest = value
    return latest


def fetch_range(series: FxSeries, start: date, end: date) -> list:
    if start > end:
        raise ValueError(
            "window for {} starts at {} and ends at {}".format(
                series.currency, start, end
            )
        )
    response = requests.get(
        SGS_URL.format(series.code),
        params={
            "formato": "json",
            "dataInicial": _brazilian(start),
            "dataFinal": _brazilian(end),
        },
        timeout=request_timeout(),
    )
    if response.status_code == 404:
        return []
    response.raise_for_status()
    try:
        return response.json()
    except ValueError as error:
        raise ContractViolation(
            "series {} returned {} bytes that are not JSON: {!r}".format(
                series.code, len(response.content), response.text[:120]
            )
        ) from error


def to_observations(payload, series: FxSeries, ingested_at: datetime) -> list:
    if not isinstance(payload, list):
        raise ContractViolation(
            "series {} returned {} where a list of observations was expected".format(
                series.code, type(payload).__name__
            )
        )
    records = []
    for entry in payload:
        try:
            observation = SgsObservation.model_validate(entry)
        except ValidationError as error:
            problem = error.errors()[0]
            raise ContractViolation(
                "series {} returned an observation that breaks the contract: {}: {}".format(
                    series.code,
                    ".".join(str(part) for part in problem["loc"]) or "row",
                    problem["msg"],
                )
            ) from error
        records.append(
            {
                "source": SOURCE,
                "ingested_at": ingested_at,
                "series_code": series.code,
                "currency": series.currency,
                "quote_date": observation.data,
                "rate_brl": observation.valor,
            }
        )
    return records


def write_month(root: Path, series: FxSeries, month: date, records: list) -> Path:
    directory = _month_directory(root, series, month)
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / FILE_NAME
    staging = directory / STAGING_NAME
    table = pa.Table.from_pylist(records, schema=SCHEMA)
    with staging.open("wb") as stream:
        pq.write_table(table, stream, compression="zstd")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(staging, target)
    return target


def resume_from(root: Path, series: FxSeries) -> date:
    latest = watermark(root, series)
    if latest is None:
        return default_start()
    return latest.replace(day=1)


def extract(root: Path, series: FxSeries, start: date, end: date, fetch=fetch_range) -> int:
    written = 0
    for window_start, window_end in month_windows(start, end):
        records = to_observations(
            fetch(series, window_start, window_end),
            series,
            datetime.now(timezone.utc),
        )
        if not records:
            continue
        write_month(root, series, window_start, records)
        written += len(records)
    return written


def main() -> int:
    load_env_file()
    root = bronze_root()
    today = date.today()
    end = date.fromisoformat(os.environ["FX_END_DATE"]) if os.environ.get("FX_END_DATE") else today
    override = os.environ.get("FX_START_DATE")
    if end > today:
        print(
            "refusing to request quotes up to {}, which is in the future".format(end),
            file=sys.stderr,
        )
        return 2
    total = 0
    for series in FX_SERIES:
        start = date.fromisoformat(override) if override else resume_from(root, series)
        if start > end:
            print(
                "{} is already current through {}".format(series.currency, start),
                file=sys.stderr,
            )
            continue
        written = extract(root, series, start, end)
        total += written
        print(
            "{}: {} observations from {} to {}".format(
                series.currency, written, start, end
            ),
            file=sys.stderr,
        )
    print("wrote {} observations".format(total), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
