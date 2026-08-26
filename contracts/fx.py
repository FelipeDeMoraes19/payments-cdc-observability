import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Annotated

from pydantic import BaseModel, BeforeValidator, ConfigDict

BRAZILIAN_DATE = re.compile(r"^(\d{2})/(\d{2})/(\d{4})$")


def parse_brazilian_date(value):
    if not isinstance(value, str):
        return value
    match = BRAZILIAN_DATE.match(value.strip())
    if not match:
        raise ValueError(
            "expected a date formatted as DD/MM/YYYY, got {!r}".format(value)
        )
    day, month, year = match.groups()
    return date(int(year), int(month), int(day))


BrazilianDate = Annotated[date, BeforeValidator(parse_brazilian_date)]


class SgsObservation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    data: BrazilianDate
    valor: Decimal


@dataclass(frozen=True)
class FxSeries:
    currency: str
    code: int
    description: str


FX_SERIES = (
    FxSeries("USD", 1, "US dollar, PTAX sell rate"),
    FxSeries("EUR", 21619, "Euro, PTAX sell rate"),
)
