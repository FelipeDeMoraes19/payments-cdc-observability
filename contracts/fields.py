import re
from datetime import datetime
from typing import Annotated

from pydantic import BeforeValidator

TWO_DIGIT_OFFSET = re.compile(r"([+-]\d{2})$")


def normalize_offset(value):
    if isinstance(value, str):
        return TWO_DIGIT_OFFSET.sub(r"\g<1>:00", value)
    return value


PostgresTimestamp = Annotated[datetime, BeforeValidator(normalize_offset)]
