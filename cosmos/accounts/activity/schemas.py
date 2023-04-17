from datetime import date, datetime
from typing import TYPE_CHECKING

from pydantic import BaseModel, Extra, Field, NonNegativeInt, validator

from cosmos.accounts.api.schemas.utils import UTCDatetime

if TYPE_CHECKING:
    from uuid import UUID


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


class BalanceResetDataSchema(BalanceChangeDataSchema):
    reset_date: date | None

    @validator("reset_date", pre=False, always=True)
    @classmethod
    def format_date(cls, value: date | None) -> str | None:
        return value.isoformat() if value else None


class EmailEventActivityDataSchema(BaseModel):
    time: int = Field(..., alias="event_datetime")
    Message_GUID: str = Field(..., alias="message_uuid")

    @validator("time", pre=True, always=True)
    @classmethod
    def timestamp_from_datetime(cls, v: datetime) -> int:
        return int(v.timestamp())

    @validator("Message_GUID", pre=True, always=True)
    @classmethod
    def string_from_uuid(cls, v: "UUID") -> str:
        return str(v)

    class Config:
        extra = Extra.allow


class SendEmailDataSchema(BaseModel):
    notification_type: str
    retailer_slug: str
    reward_slug: str | None
    template_id: int
    balance_reset_date: str | None
    account_holder_joined_date: datetime | None
    reward_issued_date: datetime | None
