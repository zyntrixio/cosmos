from datetime import datetime, timezone
from uuid import UUID

from cosmos_message_lib.schemas import ActivitySchema


class ActivityTypeMixin:
    @classmethod
    def _assemble_payload(
        cls,
        activity_type: str,
        *,
        underlying_datetime: datetime,
        summary: str,
        associated_value: str,
        retailer_slug: str,
        data: dict,
        activity_identifier: str | None = "N/A",
        reasons: list[str] | None = None,
        campaigns: list[str] | None = None,
        user_id: UUID | str | None = None,
    ) -> dict:
        return ActivitySchema(
            type=activity_type,
            datetime=datetime.now(tz=timezone.utc),
            underlying_datetime=underlying_datetime,
            summary=summary,
            reasons=reasons or [],
            activity_identifier=activity_identifier or "N/A",
            user_id=user_id,
            associated_value=associated_value,
            retailer=retailer_slug,
            campaigns=campaigns or [],
            data=data,
        ).dict(exclude_unset=True)
