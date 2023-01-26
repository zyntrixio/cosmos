from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING

from cosmos.campaigns.activity.schemas import BalanceChangeActivityDataSchema, CampaignStatusChangeActivitySchema
from cosmos.campaigns.enums import LoyaltyTypes
from cosmos.core.activity.enums import ActivityTypeMixin
from cosmos.core.config import settings
from cosmos.core.utils import pence_integer_to_currency_string

if TYPE_CHECKING:  # pragma: no cover

    from cosmos.campaigns.enums import CampaignStatuses


class ActivityType(ActivityTypeMixin, Enum):
    CAMPAIGN = f"activity.{settings.PROJECT_NAME}.campaign.status.change"
    BALANCE_CHANGE = f"activity.{settings.PROJECT_NAME}.balance.change"

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
            underlying_datetime=updated_at,
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

    @classmethod
    def get_balance_change_activity_data(
        cls,
        *,
        retailer_slug: str,
        from_campaign_slug: str,
        to_campaign_slug: str,
        account_holder_uuid: str,
        activity_datetime: datetime,
        new_balance: int,
        loyalty_type: LoyaltyTypes,
    ) -> dict:

        match loyalty_type:  # noqa: E999
            case LoyaltyTypes.STAMPS:
                stamp_balance = new_balance // 100
                associated_value = f"{stamp_balance} stamp" + ("s" if stamp_balance != 1 else "")
            case LoyaltyTypes.ACCUMULATOR:
                associated_value = pence_integer_to_currency_string(new_balance, "GBP")
            case _:
                raise ValueError(f"Unexpected value {loyalty_type} for loyalty_type.")

        return cls._assemble_payload(
            activity_type=cls.BALANCE_CHANGE.name,
            underlying_datetime=activity_datetime,
            summary=f"{retailer_slug} {to_campaign_slug} Balance {associated_value}",
            reasons=[f"Migrated from ended campaign {from_campaign_slug}"],
            activity_identifier="N/A",
            user_id=account_holder_uuid,
            associated_value=associated_value,
            retailer_slug=retailer_slug,
            campaigns=[to_campaign_slug],
            data=BalanceChangeActivityDataSchema(
                loyalty_type=loyalty_type,
                new_balance=new_balance,
                original_balance=0,
            ).dict(),
        )
