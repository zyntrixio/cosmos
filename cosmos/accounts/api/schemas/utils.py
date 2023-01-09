from datetime import datetime, timezone
from typing import Generator

from pydantic.datetime_parse import parse_datetime


# Adapted from StackOverflow: https://stackoverflow.com/questions/66548586/how-to-change-date-format-in-pydantic
class UTCDatetime(datetime):
    @classmethod
    def __get_validators__(cls) -> Generator:
        yield parse_datetime
        yield cls.ensure_tzinfo

    @classmethod
    def ensure_tzinfo(cls, v: datetime) -> datetime:

        if v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)

        return v.astimezone(timezone.utc)


def utc_datetime_from_timestamp(v: float) -> datetime:
    return datetime.fromtimestamp(v, tz=timezone.utc)
