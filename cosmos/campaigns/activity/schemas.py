from pydantic import BaseModel, NonNegativeInt, root_validator, validator

from cosmos.campaigns.enums import LoyaltyTypes


class _CampaignStatusChangeValuesSchema(BaseModel):
    status: str


class _CampaignStatusChangeDataSchema(BaseModel):
    new_values: _CampaignStatusChangeValuesSchema
    original_values: _CampaignStatusChangeValuesSchema


class CampaignStatusChangeActivitySchema(BaseModel):
    campaign: _CampaignStatusChangeDataSchema


class BalanceChangeActivityDataSchema(BaseModel):
    loyalty_type: LoyaltyTypes
    new_balance: NonNegativeInt
    original_balance: NonNegativeInt

    @validator("loyalty_type", always=True, pre=False)
    @classmethod
    def return_enum_name(cls, value: LoyaltyTypes) -> str:
        return value.name


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
