from enum import Enum
from typing import TYPE_CHECKING

from cosmos.core.activity.enums import ActivityTypeMixin
from cosmos.public.activity.schemas import EmailEventActivityDataSchema
from cosmos.public.config import public_settings

if TYPE_CHECKING:
    from datetime import datetime
    from uuid import UUID


class ActivityType(ActivityTypeMixin, Enum):

    EMAIL_EVENT = f"activity.{public_settings.core.PROJECT_NAME}.email.event"

    @classmethod
    def get_email_event_activity_data(
        cls,
        *,
        event: str,
        message_uuid: "UUID",
        underlying_timestamp: "datetime",
        retailer_slug: str,
        account_holder_uuid: "UUID",
        payload: dict,
    ) -> dict:

        return cls._assemble_payload(
            activity_type=ActivityType.EMAIL_EVENT.name,
            underlying_datetime=underlying_timestamp,
            summary=f"{event} Mailjet event received",
            reasons=["MailJet Event received"],
            activity_identifier=str(message_uuid),
            user_id=str(account_holder_uuid),
            associated_value=event,
            retailer_slug=retailer_slug,
            campaigns=[],
            data=EmailEventActivityDataSchema(**payload).dict(),
        )
