import re
import uuid

from datetime import datetime
from typing import TYPE_CHECKING, Any, no_type_check

from pydantic import UUID4, BaseModel, EmailStr, Extra, Field, StrictInt, constr, validator
from pydantic.validators import str_validator

from cosmos.accounts.enums import AccountHolderStatuses
from cosmos.core.api.service import ServiceError
from cosmos.core.error_codes import ErrorCode
from cosmos.db.models import PendingReward, Reward
from cosmos.retailers.enums import RetailerStatuses

from .utils import UTCDatetime, utc_datetime_from_timestamp

if TYPE_CHECKING:  # pragma: no cover
    from pydantic.typing import CallableGenerator

    from cosmos.db.models import CampaignBalance, Transaction, TransactionEarn

strip_currency_re = re.compile(r"^(-)?[^0-9-]?([0-9,.]+)[^0-9-]*$")


class AccountHolderUUIDValidator(UUID4):
    @classmethod
    def __get_validators__(cls) -> "CallableGenerator":
        yield str_validator
        yield cls.validate

    @classmethod
    def validate(cls, value: str) -> UUID4:
        try:
            v = UUID4(value)
        except ValueError:
            raise ServiceError(error_code=ErrorCode.NO_ACCOUNT_FOUND) from None
        else:
            return v


class MarketingPreference(BaseModel):
    key: str
    value: Any = Field(...)

    extra = Extra.forbid


class AccountHolderEnrolment(BaseModel):
    credentials: dict
    marketing_preferences: list[MarketingPreference]
    callback_url: str
    third_party_identifier: constr(min_length=1, max_length=200, strip_whitespace=True)  # type: ignore [valid-type]


class AccountHolderRewardSchema(BaseModel):
    code: str
    campaign_slug: str
    issued_date: UTCDatetime
    redeemed_date: UTCDatetime | None
    expiry_date: UTCDatetime  # expiry_date must be declared before status
    status: Reward.RewardStatuses | None = None

    @validator("issued_date", "redeemed_date", "expiry_date", allow_reuse=True)
    @classmethod
    def get_timestamp(cls, dt: datetime | None) -> int | None:
        return int(dt.timestamp()) if dt else None

    @no_type_check
    @classmethod
    def from_orm(cls, obj: Reward) -> BaseModel:
        obj.campaign_slug = obj.campaign.slug
        return super().from_orm(obj)

    class Config:
        orm_mode = True
        arbitrary_types_allowed = True


class PendingRewardAllocationSchema(BaseModel):
    created_date: float
    conversion_date: float
    value: int
    campaign_slug: str
    reward_slug: str
    count: int = 1
    total_cost_to_user: int | None

    _utc_datetimes_from_timestamps = validator("created_date", "conversion_date", allow_reuse=True)(
        utc_datetime_from_timestamp
    )


class PendingRewardAllocationResponseSchema(BaseModel):
    pending_reward_items: list[dict]

    @classmethod
    def from_orm(cls, obj: PendingReward) -> BaseModel:  # type: ignore [override]
        pending_reward_items = [
            {
                "created_date": int(obj.created_date.timestamp()),
                "conversion_date": int(obj.conversion_date.timestamp()),
                "campaign_slug": obj.campaign.slug,
            }
        ] * obj.count

        setattr(obj, "pending_reward_items", pending_reward_items)  # noqa: B010
        return super().from_orm(obj)

    class Config:
        orm_mode = True


class CampaignBalanceSchema(BaseModel):
    balance: int = Field(..., alias="value")
    campaign_slug: str

    @validator("balance")
    @classmethod
    def get_float(cls, v: int) -> float:
        return v / 100

    @classmethod
    def from_orm(cls, obj: "CampaignBalance") -> BaseModel:  # type: ignore [override]
        obj.campaign_slug = obj.campaign.slug  # type: ignore [attr-defined]
        return super().from_orm(obj)

    class Config:
        orm_mode = True
        allow_population_by_field_name = True


class TransactionHistorySchema(BaseModel):
    datetime_: int = Field(..., alias="datetime")
    amount: str
    amount_currency: str
    location: str = "N/A"
    loyalty_earned_value: str | None = None
    loyalty_earned_type: str | None = None

    @validator("datetime_", pre=True)
    @classmethod
    def get_timestamp(cls, value: datetime) -> int:
        return int(value.timestamp())

    @no_type_check
    @classmethod
    def from_orm(cls, obj: "Transaction") -> BaseModel:
        obj.amount_currency = "GBP"
        if hasattr(obj, "store") and obj.store is not None:
            obj.location = obj.store.store_name
        if obj.transaction_earn:
            transaction_earn: "TransactionEarn" = obj.transaction_earn
            obj.loyalty_earned_value = transaction_earn.humanized_earn_amount()
            obj.loyalty_earned_type = transaction_earn.loyalty_type.name
            obj.amount = obj.humanized_transaction_amount()

        return super().from_orm(obj)

    class Config:
        orm_mode = True
        allow_population_by_field_name = True


class _Retailer(BaseModel):
    status: RetailerStatuses

    class Config:
        orm_mode = True


class AccountHolderResponseSchema(BaseModel):
    retailer: _Retailer = Field(..., exclude=True)
    account_holder_uuid: UUID4 = Field(..., alias="UUID")
    status: AccountHolderStatuses
    email: str
    account_number: str | None
    current_balances: list[CampaignBalanceSchema] = []
    transactions: list[TransactionHistorySchema] = Field([], alias="transaction_history")
    rewards: list[AccountHolderRewardSchema] = []
    pending_rewards: list[PendingRewardAllocationResponseSchema] = []

    @validator("pending_rewards")
    @classmethod
    def format_pending_rewards_data(cls, pending_rewards: list[PendingRewardAllocationResponseSchema]) -> list:
        return [row for pending_reward in pending_rewards for row in pending_reward.pending_reward_items]

    @validator("transactions")
    @classmethod
    def order_transactions(cls, transactions: list[TransactionHistorySchema]) -> list[TransactionHistorySchema]:
        return sorted(transactions, key=lambda t: t.datetime_, reverse=True)

    @validator("current_balances", always=True)
    @classmethod
    def add_no_balances_object(cls, value: list[dict], values: dict) -> list[dict]:
        if values["retailer"].status == RetailerStatuses.TEST and not value:
            return [
                {
                    "campaign_slug": "N/A",
                    "value": 0,
                }
            ]

        return value

    class Config:
        orm_mode = True
        allow_population_by_field_name = True
        arbitrary_types_allowed = True


class GetAccountHolderByCredentials(BaseModel):
    email: EmailStr
    account_number: str


class AccountHolderAdjustmentSchema(BaseModel):
    balance_change: StrictInt
    campaign_slug: str
    transaction_datetime: float
    reason: str
    is_transaction: bool = True

    _utc_datetimes_from_timestamps = validator("transaction_datetime", allow_reuse=True)(utc_datetime_from_timestamp)


class AccountHolderAddRewardSchema(BaseModel):
    reward_uuid: uuid.UUID
    code: constr(min_length=1, max_length=100, strip_whitespace=True)  # type: ignore
    reward_slug: str
    campaign_slug: str
    issued_date: float
    expiry_date: float
    associated_url: str

    _utc_datetimes_from_timestamps = validator("issued_date", "expiry_date", allow_reuse=True)(
        utc_datetime_from_timestamp
    )


class AccountHolderUpdateStatusSchema(BaseModel):
    status: AccountHolderStatuses
