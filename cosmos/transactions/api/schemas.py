from datetime import datetime, timezone
from typing import Generator

from pydantic import BaseModel, Field, StrictInt
from pydantic.types import UUID4
from pydantic.validators import float_validator


class DateTime(datetime):
    @classmethod
    def __get_validators__(cls) -> Generator:
        yield float_validator
        yield cls.convert_float_to_datetime

    @classmethod
    def convert_float_to_datetime(cls, value: float) -> datetime:
        try:
            return datetime.fromtimestamp(value, tz=timezone.utc)
        except TypeError as ex:
            raise ValueError("invalid datetime") from ex


class CreateTransactionSchema(BaseModel):  # pragma: no cover
    transaction_id: str = Field(..., min_length=1, alias="id")
    payment_transaction_id: str = Field(..., min_length=1, alias="transaction_id")
    amount: StrictInt = Field(..., alias="transaction_total")
    transaction_datetime: DateTime = Field(..., alias="datetime")
    mid: str = Field(..., min_length=1, alias="MID")
    account_holder_uuid: UUID4 = Field(..., alias="loyalty_id")

    class Config:
        anystr_strip_whitespace = True
