from pydantic import BaseModel, Field

from cosmos.campaigns.enums import CampaignStatuses
from cosmos.rewards.enums import PendingRewardActions


class ActivityMetadataSchema(BaseModel):
    sso_username: str


class CampaignsStatusChangeSchema(BaseModel):
    requested_status: CampaignStatuses
    activity_metadata: ActivityMetadataSchema
    campaign_slug: str = Field(..., min_length=1)
    pending_rewards_action: PendingRewardActions = PendingRewardActions.REMOVE

    class Config:
        anystr_strip_whitespace = True
