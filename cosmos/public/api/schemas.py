from datetime import date

from pydantic import BaseModel, Field, validator

from cosmos.db.models import Reward, RewardConfig


class RewardConfigSchema(BaseModel):
    slug: str

    class Config:
        orm_mode = True


class RewardMicrositeResponseSchema(BaseModel):
    code: str
    expiry_date: date
    reward_config: RewardConfigSchema = Field(..., alias="template_slug")
    status: Reward.RewardStatuses
    redeemed_date: date | None

    @validator("reward_config")
    @classmethod
    def format_reward_config(cls, value: RewardConfig) -> str:
        return value.slug

    class Config:
        orm_mode = True
        allow_population_by_field_name = True
