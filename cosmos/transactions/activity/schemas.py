from cosmos_message_lib.schemas import utc_datetime
from pydantic import BaseModel, NonNegativeInt, validator

from cosmos.campaigns.enums import LoyaltyTypes


class EarnedSchema(BaseModel):
    value: str
    type: LoyaltyTypes  # noqa: A003

    @validator("type")
    @classmethod
    def convert_type(cls, value: LoyaltyTypes) -> str:
        return value.name


class ProcessedTXEventSchema(BaseModel):
    transaction_id: str
    datetime: utc_datetime
    amount: str
    amount_currency: str
    store_name: str
    earned: list[EarnedSchema]
    mid: str


class TxImportEventSchema(BaseModel):
    transaction_id: str
    datetime: utc_datetime
    amount: str
    mid: str


class RefundNotRecoupedDataSchema(BaseModel):
    datetime: utc_datetime
    transaction_id: str | None
    amount: int
    amount_recouped: int
    amount_not_recouped: NonNegativeInt
