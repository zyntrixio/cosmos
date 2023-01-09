from enum import Enum
from typing import TYPE_CHECKING

from cosmos.campaigns.activity.schemas import CampaignStatusChangeActivitySchema
from cosmos.core.activity.enums import ActivityTypeMixin
from cosmos.core.config import settings

if TYPE_CHECKING:  # pragma: no cover
    from datetime import datetime

    from cosmos.campaigns.enums import CampaignStatuses


class ActivityType(ActivityTypeMixin, Enum):
    CAMPAIGN = f"activity.{settings.PROJECT_NAME}.campaign.status.change"

    @classmethod
    def get_campaign_status_change_activity_data(
        cls,
        *,
        updated_at: "datetime",
        campaign_name: str,
        campaign_slug: str,
        retailer_slug: str,
        sso_username: str,
        original_status: "CampaignStatuses",
        new_status: "CampaignStatuses",
    ) -> dict:

        return cls._assemble_payload(
            ActivityType.CAMPAIGN.name,
            activity_datetime=updated_at,
            summary=f"{campaign_name} {new_status.value}",
            reasons=[],
            activity_identifier=campaign_slug,
            user_id=sso_username,
            associated_value=new_status.value,
            retailer_slug=retailer_slug,
            campaigns=[campaign_slug],
            data=CampaignStatusChangeActivitySchema(
                campaign={
                    "new_values": {
                        "status": new_status.value,
                    },
                    "original_values": {
                        "status": original_status.value,
                    },
                }
            ).dict(exclude_unset=True),
        )
