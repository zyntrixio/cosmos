from pydantic import BaseModel, NonNegativeInt, validator

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
