from datetime import date, datetime, timezone
from decimal import Decimal

import pyarrow.parquet as pq
import pytest
import requests

from contracts.fx import FX_SERIES, FxSeries
from contracts.validation import ContractViolation
from ingestion.batch import bcb_fx

USD = FX_SERIES[0]


def fake_fetch(quotes):
    def fetch(series: FxSeries, start: date, end: date) -> list:
        return [
            {"data": day.strftime("%d/%m/%Y"), "valor": value}
            for day, value in quotes
            if start <= day <= end
        ]

    return fetch


def observations(root) -> list:
    rows = []
    for path in sorted(root.rglob("*.parquet")):
        table = pq.read_table(path, columns=["currency", "quote_date", "rate_brl"])
        rows.extend(
            (row["currency"], row["quote_date"], str(row["rate_brl"]))
            for row in table.to_pylist()
        )
    return sorted(rows)


def test_month_windows_split_the_range_and_clip_the_last_one():
    windows = bcb_fx.month_windows(date(2026, 6, 15), date(2026, 8, 26))
    assert windows == [
        (date(2026, 6, 1), date(2026, 6, 30)),
        (date(2026, 7, 1), date(2026, 7, 31)),
        (date(2026, 8, 1), date(2026, 8, 26)),
    ]


def test_a_rerun_over_the_same_window_changes_no_observation(tmp_path):
    quotes = [(date(2026, 6, 1), "5.01"), (date(2026, 7, 2), "5.02")]
    for _ in range(2):
        bcb_fx.extract(
            tmp_path, USD, date(2026, 6, 1), date(2026, 7, 31), fetch=fake_fetch(quotes)
        )
    files = sorted(path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob("*.parquet"))
    assert files == [
        "USD/month=2026-06/observations.parquet",
        "USD/month=2026-07/observations.parquet",
    ]
    assert observations(tmp_path) == [
        ("USD", date(2026, 6, 1), "5.01000000"),
        ("USD", date(2026, 7, 2), "5.02000000"),
    ]


def test_the_watermark_resumes_from_the_month_of_the_last_observation(tmp_path):
    quotes = [(date(2026, 6, 1), "5.01"), (date(2026, 7, 2), "5.02")]
    bcb_fx.extract(
        tmp_path, USD, date(2026, 6, 1), date(2026, 7, 31), fetch=fake_fetch(quotes)
    )
    assert bcb_fx.watermark(tmp_path, USD) == date(2026, 7, 2)
    assert bcb_fx.resume_from(tmp_path, USD) == date(2026, 7, 1)


def test_an_empty_window_writes_no_file(tmp_path):
    written = bcb_fx.extract(
        tmp_path, USD, date(2026, 6, 1), date(2026, 6, 30), fetch=fake_fetch([])
    )
    assert written == 0
    assert list(tmp_path.rglob("*.parquet")) == []


def test_a_payload_that_breaks_the_contract_fails_loudly():
    stamp = datetime.now(timezone.utc)
    with pytest.raises(ContractViolation) as unexpected_field:
        bcb_fx.to_observations([{"data": "01/06/2026", "valor": "5.01", "extra": 1}], USD, stamp)
    assert "extra" in str(unexpected_field.value)

    with pytest.raises(ContractViolation) as wrong_date:
        bcb_fx.to_observations([{"data": "2026-06-01", "valor": "5.01"}], USD, stamp)
    assert "data" in str(wrong_date.value)

    with pytest.raises(ContractViolation) as not_a_list:
        bcb_fx.to_observations({"erro": "SGSNegocioException"}, USD, stamp)
    assert "list of observations" in str(not_a_list.value)


def test_the_live_api_still_satisfies_the_contract():
    try:
        payload = bcb_fx.fetch_range(USD, date(2026, 8, 3), date(2026, 8, 5))
    except requests.RequestException as error:
        pytest.skip("BCB API unreachable: {}".format(error))
    records = bcb_fx.to_observations(payload, USD, datetime.now(timezone.utc))
    assert records, "the BCB returned no quotes for a window of business days"
    for record in records:
        assert record["currency"] == "USD"
        assert isinstance(record["rate_brl"], Decimal)
        assert record["quote_date"].year == 2026
