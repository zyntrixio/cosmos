import re
import uuid

from datetime import datetime, timezone
from typing import Any

from pydantic import UUID4, BaseModel, EmailStr, Extra, Field, StrictInt, constr, validator

from cosmos.accounts.enums import AccountHolderStatuses, RewardApiStatuses
from cosmos.db.models import AccountHolderPendingReward

from .utils import utc_datetime, utc_datetime_from_timestamp

strip_currency_re = re.compile(r"^(-)?[^0-9-]?([0-9,.]+)[^0-9-]*$")


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
    issued_date: utc_datetime
    redeemed_date: utc_datetime | None
    expiry_date: utc_datetime  # expiry_date must be declared before status
    status: RewardApiStatuses | None = None

    @classmethod
    def from_orm(cls, obj: Any) -> BaseModel:  # type: ignore [override]
        obj.campaign_slug = obj.campaign.slug
        if obj.redeemed_date:
            obj.status = RewardApiStatuses.REDEEMED
        elif obj.expiry_date.replace(tzinfo=timezone.utc) < datetime.now(tz=timezone.utc):
            obj.status = RewardApiStatuses.EXPIRED
        elif obj.cancelled_date:
            obj.status = RewardApiStatuses.CANCELLED
        else:
            obj.status = RewardApiStatuses.ISSUED

        return super().from_orm(obj)

    @validator("issued_date", "redeemed_date", "expiry_date", allow_reuse=True)
    @classmethod
    def get_timestamp(cls, dt: datetime | None) -> int | None:
        return int(dt.timestamp()) if dt else None

    class Config:
        orm_mode = True


class AccountHolderPendingRewardAllocationSchema(BaseModel):
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


class AccountHolderPendingRewardAllocationResponseSchema(BaseModel):
    pending_reward_items: list[dict]

    @classmethod
    def from_orm(cls, obj: AccountHolderPendingReward) -> BaseModel:  # type: ignore [override]
        pending_reward_items = [
            {
                "created_date": int(obj.created_date.timestamp()),
                "conversion_date": int(obj.conversion_date.timestamp()),
                "campaign_slug": obj.campaign.slug,
            }
        ] * obj.count

        setattr(obj, "pending_reward_items", pending_reward_items)
        return super().from_orm(obj)

    class Config:
        orm_mode = True


class AccountHolderCampaignBalanceSchema(BaseModel):
    balance: int = Field(..., alias="value")
    campaign_slug: str

    @validator("balance")
    @classmethod
    def get_float(cls, v: int) -> float:
        return v / 100

    @classmethod
    def from_orm(cls, obj: Any) -> BaseModel:  # type: ignore [override]
        obj.campaign_slug = obj.campaign.slug
        return super().from_orm(obj)

    class Config:
        orm_mode = True
        allow_population_by_field_name = True


class TransactionHistorySchema(BaseModel):
    datetime: int = Field(..., alias="datetime")
    amount: str
    amount_currency: str
    location_name: str | None = Field(None, alias="location")
    loyalty_earned_value: str | None = None
    loyalty_earned_type: str | None = None

    @validator("datetime", pre=True)
    @classmethod
    def get_timestamp(cls, value: datetime) -> int:  # type: ignore [valid-type]
        return int(value.timestamp())  # type: ignore [attr-defined]

    @classmethod
    def from_orm(cls, obj: Any) -> BaseModel:  # type: ignore [override]
        obj.amount_currency = "GBP"
        if hasattr(obj, "store") and obj.store is not None:
            obj.location_name = obj.store.store_name
        if obj.transaction_campaigns:
            obj.loyalty_earned_value = obj.transaction_campaigns[0].adjustment
            obj.loyalty_earned_type = obj.transaction_campaigns[0].campaign.loyalty_type.name

        return super().from_orm(obj)

    class Config:
        orm_mode = True
        allow_population_by_field_name = True


class AccountHolderStatusResponseSchema(BaseModel):
    status: AccountHolderStatuses

    class Config:
        orm_mode = True


class AccountHolderResponseSchema(AccountHolderStatusResponseSchema):
    account_holder_uuid: UUID4 = Field(..., alias="UUID")
    email: str
    account_number: str | None
    current_balances: list[AccountHolderCampaignBalanceSchema] = []
    transactions: list[TransactionHistorySchema] = Field([], alias="transaction_history")
    rewards: list[AccountHolderRewardSchema] = []
    pending_rewards: list[AccountHolderPendingRewardAllocationResponseSchema] = []

    @validator("pending_rewards")
    @classmethod
    def format_pending_rewards_data(
        cls, pending_rewards: list[AccountHolderPendingRewardAllocationResponseSchema]
    ) -> list:
        return [row for pending_reward in pending_rewards for row in pending_reward.pending_reward_items]

    @validator("transactions")
    @classmethod
    def order_transactions(cls, transactions: list[TransactionHistorySchema]) -> list[TransactionHistorySchema]:
        return sorted(transactions, key=lambda t: t.datetime, reverse=True)

    class Config:
        orm_mode = True
        allow_population_by_field_name = True


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
