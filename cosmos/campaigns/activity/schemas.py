from pydantic import BaseModel


class _CampaignStatusChangeValuesSchema(BaseModel):
    status: str


class _CampaignStatusChangeDataSchema(BaseModel):
    new_values: _CampaignStatusChangeValuesSchema
    original_values: _CampaignStatusChangeValuesSchema


class CampaignStatusChangeActivitySchema(BaseModel):
    campaign: _CampaignStatusChangeDataSchema
