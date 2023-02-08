from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING
from uuid import UUID

from cosmos.core.activity.enums import ActivityTypeMixin
from cosmos.core.config import settings
from cosmos.core.utils import pence_integer_to_currency_string
from cosmos.rewards.activity.schemas import (
    PendingRewardStatusDataSchema,
    RewardStatusDataSchema,
    RewardTransferActivityDataSchema,
    RewardUpdateDataSchema,
)

if TYPE_CHECKING:
    from cosmos.db.models import Campaign, Retailer


class IssuedRewardReasons(Enum):
    CAMPAIGN_END = "Pending reward converted at campaign end"
    CONVERTED = "Pending Reward converted"
    GOAL_MET = "Reward goal met"


class ActivityType(ActivityTypeMixin, Enum):
    REWARD_STATUS = f"activity.{settings.PROJECT_NAME}.reward.status"
    REWARD_UPDATE = f"activity.{settings.PROJECT_NAME}.reward.update"

    @classmethod
    def get_pending_reward_deleted_activity_data(
        cls,
        *,
        retailer_slug: str,
        campaign_slug: str,
        account_holder_uuid: UUID | str,
        pending_reward_uuid: UUID | str,
        activity_datetime: datetime,
    ) -> dict:
        return cls._assemble_payload(
            activity_type=cls.REWARD_STATUS.name,
            underlying_datetime=activity_datetime,
            summary=f"{retailer_slug} Pending Reward removed for {campaign_slug}",
            reasons=["Pending Reward removed due to campaign end/cancellation"],
            user_id=account_holder_uuid,
            activity_identifier=str(pending_reward_uuid),
            associated_value="Deleted",
            retailer_slug=retailer_slug,
            campaigns=[campaign_slug],
            data=PendingRewardStatusDataSchema(
                new_status="deleted",
                original_status="pending",
            ).dict(exclude_unset=True),
        )

    @classmethod
    def get_pending_reward_transferred_activity_data(
        cls,
        *,
        retailer_slug: str,
        from_campaign_slug: str,
        to_campaign_slug: str,
        account_holder_uuid: str,
        activity_datetime: datetime,
        pending_reward_uuid: str,
    ) -> dict:

        return cls._assemble_payload(
            activity_type=cls.REWARD_STATUS.name,
            underlying_datetime=activity_datetime,
            summary=f"{retailer_slug} pending reward transferred from {from_campaign_slug} to {to_campaign_slug}",
            reasons=["Pending reward transferred at campaign end"],
            activity_identifier=pending_reward_uuid,
            user_id=account_holder_uuid,
            associated_value="N/A",
            retailer_slug=retailer_slug,
            campaigns=[from_campaign_slug, to_campaign_slug],
            data=RewardTransferActivityDataSchema(
                new_campaign=to_campaign_slug,
                old_campaign=from_campaign_slug,
            ).dict(),
        )

    @classmethod
    def get_reward_status_activity_data(
        cls,
        *,
        account_holder_uuid: UUID | str,
        retailer_slug: str,
        summary: str,
        new_status: str,
        campaigns: list[str] | None = None,
        reason: str | None = None,
        activity_datetime: datetime,
        original_status: str | None = None,
        activity_identifier: str | None = None,
        count: int | None = None,
    ) -> dict:
        data_kwargs: dict[str, str | int] = {"new_status": new_status}
        if original_status:
            data_kwargs["original_status"] = original_status
        if count:
            data_kwargs["count"] = count

        return cls._assemble_payload(
            cls.REWARD_STATUS.name,
            user_id=account_holder_uuid,
            underlying_datetime=activity_datetime,
            activity_identifier=activity_identifier,
            summary=summary,
            reasons=[reason] if reason else None,
            associated_value=new_status,
            retailer_slug=retailer_slug,
            campaigns=campaigns,
            data=PendingRewardStatusDataSchema(**data_kwargs).dict(
                exclude_unset=True,
                exclude_none=True,
            ),
        )

    @classmethod
    def get_reward_update_activity_data(
        cls,
        *,
        account_holder_uuid: UUID | str,
        retailer_slug: str,
        summary: str,
        campaigns: list[str] | None = None,
        reason: str | None = None,
        activity_datetime: datetime,
        activity_identifier: str | None = None,
        reward_update_data: dict,
    ) -> dict:
        return cls._assemble_payload(
            cls.REWARD_UPDATE.name,
            user_id=account_holder_uuid,
            underlying_datetime=activity_datetime,
            activity_identifier=activity_identifier,
            summary=summary,
            reasons=[reason] if reason else None,
            associated_value=pence_integer_to_currency_string(reward_update_data["new_total_cost_to_user"], "GBP"),
            retailer_slug=retailer_slug,
            campaigns=campaigns,
            data=RewardUpdateDataSchema(**reward_update_data).dict(exclude_unset=True),
        )

    @classmethod
    def get_issued_reward_status_activity_data(
        cls,
        *,
        account_holder_uuid: str,
        retailer: "Retailer",
        reward_slug: str,
        activity_timestamp: datetime,
        reward_uuid: str,
        pending_reward_id: str | None,
        campaign: "Campaign | None",
        reason: IssuedRewardReasons,
    ) -> dict:
        data_payload = {"new_status": "issued", "reward_slug": reward_slug}

        if reason in (IssuedRewardReasons.CONVERTED, IssuedRewardReasons.CAMPAIGN_END):
            if not (campaign and pending_reward_id):
                raise ValueError("Pending reward conversion requires a campaign and pending_reward_id")

            summary = f"{retailer.name} Pending Reward issued for {campaign.name}"
            data_payload["original_status"] = "pending"
            data_payload["pending_reward_id"] = pending_reward_id

        else:
            summary = f"{retailer.name} Reward issued"

        return cls._assemble_payload(
            activity_type=cls.REWARD_STATUS.name,
            underlying_datetime=activity_timestamp,
            summary=summary,
            reasons=[reason.value],
            activity_identifier=reward_uuid,
            user_id=account_holder_uuid,
            associated_value="issued",
            retailer_slug=retailer.slug,
            campaigns=[campaign.slug] if campaign else [],
            data=RewardStatusDataSchema(**data_payload).dict(exclude_unset=True),
        )
