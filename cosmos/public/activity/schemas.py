from datetime import datetime
from typing import TYPE_CHECKING

from pydantic import BaseModel, Extra, Field, validator

if TYPE_CHECKING:
    from uuid import UUID


class EmailEventActivityDataSchema(BaseModel, extra=Extra.allow):
    time: int = Field(..., alias="event_datetime")
    Message_GUID: str = Field(..., alias="message_uuid")

    @validator("time", pre=True, always=True)
    @classmethod
    def timestamp_from_datetime(cls, v: datetime) -> int:
        return int(v.timestamp())

    @validator("Message_GUID", pre=True, always=True)
    @classmethod
    def string_from_uuid(cls, v: "UUID") -> str:
        return str(v)
