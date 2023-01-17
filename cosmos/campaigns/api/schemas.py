from pydantic import BaseModel, Field

from cosmos.campaigns.enums import CampaignStatuses
from cosmos.rewards.enums import PendingRewardActions, PendingRewardMigrationActions


class ActivityMetadataSchema(BaseModel):
    sso_username: str


class CampaignsStatusChangeSchema(BaseModel):
    requested_status: CampaignStatuses
    activity_metadata: ActivityMetadataSchema
    campaign_slug: str = Field(..., min_length=1)
    pending_rewards_action: PendingRewardActions = PendingRewardActions.REMOVE

    class Config:
        anystr_strip_whitespace = True


class _BalanceMigrationSchema(BaseModel):
    transfer: bool
    conversion_rate: int = Field(..., le=100, ge=0)
    qualifying_threshold: int = Field(..., le=100, ge=0)


class CampaignsMigrationSchema(BaseModel):
    to_campaign: str = Field(..., min_length=1)
    from_campaign: str = Field(..., min_length=1)
    pending_rewards_action: PendingRewardMigrationActions
    balance_action: _BalanceMigrationSchema
    activity_metadata: ActivityMetadataSchema

    class Config:
        anystr_strip_whitespace = True
