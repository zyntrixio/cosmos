from datetime import datetime
from enum import Enum
from uuid import UUID

from cosmos.core.activity.enums import ActivityTypeMixin
from cosmos.core.activity.utils import pence_integer_to_currency_string
from cosmos.core.config import settings
from cosmos.rewards.activity.schemas import RewardStatusDataSchema, RewardUpdateDataSchema


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
            activity_datetime=activity_datetime,
            summary=f"{retailer_slug} Pending Reward removed for {campaign_slug}",
            reasons=["Pending Reward removed due to campaign end/cancellation"],
            user_id=account_holder_uuid,
            activity_identifier=str(pending_reward_uuid),
            associated_value="Deleted",
            retailer_slug=retailer_slug,
            campaigns=[campaign_slug],
            data=RewardStatusDataSchema(
                new_status="deleted",
                original_status="pending",
            ).dict(exclude_unset=True),
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
        data_kwargs = {"new_status": new_status, "count": count}
        if original_status:
            data_kwargs["original_status"] = original_status

        if count:
            data_kwargs["count"] = count

        return cls._assemble_payload(
            cls.REWARD_STATUS.name,
            user_id=account_holder_uuid,
            activity_datetime=activity_datetime,
            activity_identifier=activity_identifier,
            summary=summary,
            reasons=[reason] if reason else None,
            associated_value=new_status,
            retailer_slug=retailer_slug,
            campaigns=campaigns,
            data=RewardStatusDataSchema(**data_kwargs).dict(exclude_unset=True),
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
            activity_datetime=activity_datetime,
            activity_identifier=activity_identifier,
            summary=summary,
            reasons=[reason] if reason else None,
            associated_value=pence_integer_to_currency_string(reward_update_data["new_total_cost_to_user"], "GBP"),
            retailer_slug=retailer_slug,
            campaigns=campaigns,
            data=RewardUpdateDataSchema(**reward_update_data).dict(exclude_unset=True),
        )
