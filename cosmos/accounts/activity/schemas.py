from cosmos_message_lib.schemas import utc_datetime
from pydantic import BaseModel, Field, NonNegativeInt


class EarnedSchema(BaseModel):
    value: str
    type: str  # noqa: A003


class TransactionHistorySchema(BaseModel):
    transaction_id: str
    datetime: utc_datetime
    amount: str
    amount_currency: str
    location_name: str = Field(..., alias="store_name")
    earned: list[EarnedSchema]


class AccountEventSchema(BaseModel):
    channel: str
    datetime: utc_datetime


class AccountRequestSchema(AccountEventSchema):
    fields: list[dict]
    result: str | None


class MarketingPreferenceChangeSchema(BaseModel):
    field_name: str
    original_value: str
    new_value: str


class BalanceChangeDataSchema(BaseModel):
    new_balance: int
    original_balance: int


class RefundNotRecoupedDataSchema(BaseModel):
    datetime: utc_datetime
    amount: int
    amount_recouped: int
    amount_not_recouped: NonNegativeInt
