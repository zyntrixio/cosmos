from datetime import UTC, date, datetime
from uuid import UUID

from pydantic import BaseModel, Extra, Field, validator

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


class AccountHolderEmailEvent(BaseModel):
    """
    Expected Mailjet callback payload:
    ```json
    {
        "event": "sent",
        "time": 1433333949,
        "MessageID": 19421777835146490,
        "Message_GUID": "1ab23cd4-e567-8901-2345-6789f0gh1i2j",
        "email": "api@mailjet.com",
        "mj_campaign_id": 7257,
        "mj_contact_id": 4,
        "customcampaign": "",
        "mj_message_id": "19421777835146490",
        "smtp_reply": "sent (250 2.0.0 OK 1433333948 fa5si855896wjc.199 - gsmtp)",
        "CustomID": "helloworld",
    }
    """

    # NB: we will need to store the whole payload as it is in the activity data
    # so any field altered here must be reverted in the activity payload schema.
    event: str
    event_datetime: datetime = Field(..., alias="time")
    message_uuid: UUID = Field(..., alias="Message_GUID")

    @validator("event_datetime", pre=True, always=True)
    @classmethod
    def datetime_from_timestamp(cls, v: int) -> datetime:
        return datetime.fromtimestamp(v, tz=UTC)

    class Config:
        extra = Extra.allow
