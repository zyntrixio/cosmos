from datetime import datetime as dt
from datetime import timezone

from pydantic import BaseModel, Field, StrictInt, constr, validator
from pydantic.types import UUID4


# I pass in an empty string for any of these fields: id, datetime, MID or loyalty_id
class CreateTransactionSchema(BaseModel):  # pragma: no cover
    transaction_id: constr(strip_whitespace=True, min_length=1) = Field(..., alias="id")  # type: ignore [valid-type]
    payment_transaction_id: constr(strip_whitespace=True, min_length=1) = Field(  # type: ignore [valid-type]
        ..., alias="transaction_id"
    )
    amount: StrictInt = Field(..., alias="transaction_total")
    datetime: float
    mid: constr(strip_whitespace=True, min_length=1) = Field(..., alias="MID")  # type: ignore [valid-type]
    account_holder_uuid: UUID4 = Field(..., alias="loyalty_id")

    @validator("datetime")
    @classmethod
    def get_datetime_from_timestamp(cls, v: float) -> dt:
        try:
            processed_datetime = dt.fromtimestamp(v, tz=timezone.utc)
        except Exception:
            raise ValueError("invalid datetime")  # pylint: disable=raise-missing-from

        return processed_datetime
