from datetime import date

from cosmos_message_lib.schemas import ActivitySchema
from pydantic import BaseModel, Field, NonNegativeInt, validator

from cosmos.accounts.api.schemas.utils import UTCDatetime


class EarnedSchema(BaseModel):
    value: str
    type: str  # noqa: A003


class TransactionHistorySchema(BaseModel):
    transaction_id: str
    datetime: UTCDatetime
    amount: str
    amount_currency: str
    location_name: str = Field(..., alias="store_name")
    earned: list[EarnedSchema]


class AccountEventSchema(BaseModel):
    channel: str
    datetime: UTCDatetime


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
    datetime: UTCDatetime
    amount: int
    amount_recouped: int
    amount_not_recouped: NonNegativeInt


class _BalanceResetDataSchema(BalanceChangeDataSchema):
    reset_date: date | None

    @validator("reset_date", pre=False, always=True)
    @classmethod
    def format_date(cls, value: date | None) -> str | None:
        return value.isoformat() if value else None


class BalanceResetWholeDataSchema(ActivitySchema):
    data: _BalanceResetDataSchema
