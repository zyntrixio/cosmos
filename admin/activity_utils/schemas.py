from decimal import Decimal
from typing import Literal

from cosmos_message_lib.schemas import ActivitySchema, utc_datetime
from pydantic import BaseModel, Field, NonNegativeInt, root_validator, validator


def format_datetime(dt: utc_datetime | None) -> str | None:
    return dt.strftime("%Y-%m-%d %H:%M:%S") if dt else None


class _CampaignUpdatedValuesSchema(BaseModel):
    status: str | None
    name: str | None
    slug: str | None
    loyalty_type: str | None
    start_date: utc_datetime | None
    end_date: utc_datetime | None

    format_datetime = validator("start_date", "end_date", allow_reuse=True)(format_datetime)


class _CampaignUpdatedDataSchema(BaseModel):
    new_values: _CampaignUpdatedValuesSchema
    original_values: _CampaignUpdatedValuesSchema


class CampaignUpdatedActivitySchema(BaseModel):
    campaign: _CampaignUpdatedDataSchema


class _CampaignCreatedValuesSchema(_CampaignUpdatedValuesSchema):
    status: str
    name: str
    slug: str
    loyalty_type: str


class _CampaignCreatedDataSchema(BaseModel):
    new_values: _CampaignCreatedValuesSchema


class CampaignCreatedActivitySchema(BaseModel):
    campaign: _CampaignCreatedDataSchema


class _CampaignDeletedValuesSchema(BaseModel):
    retailer: str
    name: str
    slug: str
    loyalty_type: str
    start_date: utc_datetime
    end_date: utc_datetime | None

    format_datetime = validator("start_date", "end_date", allow_reuse=True)(format_datetime)


class _CampaignDeletedDataSchema(BaseModel):
    original_values: _CampaignDeletedValuesSchema


class CampaignDeletedActivitySchema(BaseModel):
    campaign: _CampaignDeletedDataSchema


class _EarnRuleUpdatedValuesSchema(BaseModel):
    threshold: int | None
    increment: int | None
    increment_multiplier: Decimal | None
    max_amount: int | None


class _EarnRuleUpdatedDataSchema(BaseModel):
    new_values: _EarnRuleUpdatedValuesSchema
    original_values: _EarnRuleUpdatedValuesSchema


class EarnRuleUpdatedActivitySchema(BaseModel):
    earn_rule: _EarnRuleUpdatedDataSchema


class _EarnRuleCreatedValuesSchema(_EarnRuleUpdatedValuesSchema):
    threshold: int
    increment_multiplier: Decimal


class _EarnRuleCreatedDataSchema(BaseModel):
    new_values: _EarnRuleCreatedValuesSchema


class EarnRuleCreatedActivitySchema(BaseModel):
    loyalty_type: str
    earn_rule: _EarnRuleCreatedDataSchema


class _EarnRuleDeletedValuesSchema(_EarnRuleCreatedValuesSchema):
    pass


class _EarnRuleDeletedDataSchema(BaseModel):
    original_values: _EarnRuleDeletedValuesSchema


class EarnRuleDeletedActivitySchema(BaseModel):
    earn_rule: _EarnRuleDeletedDataSchema


class _RewardRuleCreatedValuesSchema(BaseModel):
    campaign_slug: str
    reward_goal: int
    refund_window: int
    reward_cap: int | None


class _RewardRuleCreatedDataSchema(BaseModel):
    new_values: _RewardRuleCreatedValuesSchema


class RewardRuleCreatedActivitySchema(BaseModel):
    reward_rule: _RewardRuleCreatedDataSchema


class _BalanceChangeActivityDataSchema(BaseModel):
    loyalty_type: Literal["STAMPS", "ACCUMULATOR"]
    new_balance: NonNegativeInt
    original_balance: NonNegativeInt


class BalanceChangeWholeActivitySchema(ActivitySchema):
    """
    This will be used to send bulk messages we will use the ActivitySchema on message creation
    to skip pre send validation
    """

    data: _BalanceChangeActivityDataSchema


class _RewardStatusActivityDataSchema(BaseModel):
    new_campaign: str
    old_campaign: str


class RewardStatusWholeActivitySchema(ActivitySchema):
    """
    This will be used to send bulk messages we will use the ActivitySchema on message creation
    to skip pre send validation
    """

    data: _RewardStatusActivityDataSchema


class CampaignMigrationActivitySchema(BaseModel):
    transfer_balance_requested: bool

    ended_campaign: str
    activated_campaign: str
    balance_conversion_rate: int
    qualify_threshold: int
    pending_rewards: str

    @validator("balance_conversion_rate", "qualify_threshold", pre=False, always=True)
    @classmethod
    def convert_to_percentage_string(cls, v: int) -> str:
        return f"{v}%"

    @validator("pending_rewards", pre=False, always=True)
    @classmethod
    def convert_to_lower(cls, v: str) -> str:
        return v.lower()

    @root_validator
    @classmethod
    def format_payload(cls, values: dict) -> dict:
        if not values.pop("transfer_balance_requested"):
            values.pop("balance_conversion_rate")
            values.pop("qualify_threshold")

        return values


class _RewardRuleUpdateValuesSchema(BaseModel):
    reward_goal: int | None
    reward_slug: str | None
    campaign_slug: str | None
    refund_window: int | None = Field(alias="allocation_window")
    reward_cap: int | None


class _RewardRuleUpdatedDataSchema(BaseModel):
    new_values: _RewardRuleUpdateValuesSchema
    original_values: _RewardRuleUpdateValuesSchema


class RewardRuleUpdatedActivitySchema(BaseModel):
    reward_rule: _RewardRuleUpdatedDataSchema


class _RewardRuleDeletedValuesSchema(_RewardRuleCreatedValuesSchema):
    campaign_slug: str
    reward_cap: int | None


class _RewardRuleDeletedDataSchema(BaseModel):
    original_values: _RewardRuleDeletedValuesSchema


class RewardRuleDeletedActivitySchema(BaseModel):
    reward_rule: _RewardRuleDeletedDataSchema


class RetailerConfigCreatedDataSchema(BaseModel):
    status: str
    name: str
    slug: str
    account_number_prefix: str
    loyalty_name: str
    balance_lifespan: int
    # balance_reset_advanced_warning_days: int
    enrolment_config: list[dict]
    marketing_preference_config: list[dict] | None


class RetailerStatusUpdateDataSchema(BaseModel):
    status: str


class RetailerCreatedActivitySchema(BaseModel):
    new_values: RetailerConfigCreatedDataSchema


class RetailerStatusUpdateActivitySchema(BaseModel):
    new_values: RetailerStatusUpdateDataSchema
    original_values: RetailerStatusUpdateDataSchema


class RetailerUpdateDataSchema(BaseModel):
    status: str | None
    name: str | None
    slug: str | None
    account_number_prefix: str | None
    loyalty_name: str | None
    balance_lifespan: int | None
    balance_reset_advanced_warning_days: int | None
    enrolment_config: list[dict] | None
    marketing_preference_config: list[dict] | None


class RetailerUpdateActivitySchema(BaseModel):
    new_values: RetailerUpdateDataSchema
    original_values: RetailerUpdateDataSchema


class RetailerDeletedActivitySchema(BaseModel):
    original_values: RetailerUpdateDataSchema
