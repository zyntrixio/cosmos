from uuid import UUID

from pydantic import BaseModel


class PendingRewardStatusDataSchema(BaseModel):
    new_status: str
    original_status: str | None
    count: int | None


class RewardStatusDataSchema(BaseModel):
    new_status: str
    original_status: str | None
    reward_slug: str
    pending_reward_uuid: str | None


class RewardUpdateDataSchema(BaseModel):
    new_total_cost_to_user: int
    original_total_cost_to_user: int


class TotalCostToUserDataSchema(RewardUpdateDataSchema):
    pending_reward_id: int
    pending_reward_uuid: UUID


class RewardTransferActivityDataSchema(BaseModel):
    new_campaign: str
    old_campaign: str
