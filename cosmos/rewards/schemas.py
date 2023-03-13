from datetime import UTC, date, datetime

from pydantic import BaseModel, Extra, Field, validator

from cosmos.rewards.activity.enums import IssuedRewardReasons
from cosmos.rewards.enums import RewardUpdateStatuses


class RewardUpdateSchema(BaseModel):
    code: str
    date_: str = Field(..., alias="date")
    status: RewardUpdateStatuses

    @validator("date_")
    @classmethod
    def get_date(cls, v: str) -> date:
        return datetime.strptime(v, "%Y-%m-%d").replace(tzinfo=UTC).date()


class IssuanceTaskParams(BaseModel):
    campaign_id: int
    account_holder_id: int
    reward_config_id: int
    pending_reward_uuid: str | None
    reason: IssuedRewardReasons

    @validator("reason", pre=True, always=True)
    @classmethod
    def validate_reason(cls, v: str) -> IssuedRewardReasons:
        return IssuedRewardReasons[v]

    class Config:
        extras = Extra.ignore
