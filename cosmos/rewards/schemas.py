from datetime import date, datetime, timezone

from pydantic import BaseModel, Field, validator

from cosmos.rewards.enums import RewardUpdateStatuses


class RewardUpdateSchema(BaseModel):
    code: str
    date_: str = Field(..., alias="date")
    status: RewardUpdateStatuses

    @validator("date_")
    @classmethod
    def get_date(cls, v: str) -> date:
        return datetime.strptime(v, "%Y-%m-%d").replace(tzinfo=timezone.utc).date()
