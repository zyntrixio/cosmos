from datetime import datetime, timezone
from typing import TYPE_CHECKING

from cosmos.campaigns.activity.enums import ActivityType as CampaignActivityType
from cosmos.campaigns.api import crud
from cosmos.campaigns.api.schemas import CampaignsStatusChangeSchema
from cosmos.campaigns.enums import CampaignStatuses
from cosmos.core.activity.utils import format_and_send_activity_in_background
from cosmos.core.api.service import Service, ServiceError, ServiceResult
from cosmos.core.error_codes import ErrorCode
from cosmos.db.models import Campaign, Retailer
from cosmos.retailers.enums import RetailerStatuses
from cosmos.rewards.activity.enums import ActivityType as RewardsActivityType
from cosmos.rewards.crud import cancel_issued_rewards_for_campaign, delete_pending_rewards_for_campaign
from cosmos.rewards.enums import PendingRewardActions

if TYPE_CHECKING:  # pragma: no cover
    from fastapi import BackgroundTasks
    from sqlalchemy.ext.asyncio import AsyncSession


async def convert_pending_rewards_placeholder() -> None:  # pragma: no cover
    # TODO: complete this logic once Carina reward agents have been implemented
    pass


class CampaignService(Service):
    def __init__(self, db_session: "AsyncSession", retailer: "Retailer", background_tasks: "BackgroundTasks") -> None:
        self.background_tasks = background_tasks
        super().__init__(db_session, retailer)

    async def _check_valid_campaign(self, campaign: Campaign, requested_status: CampaignStatuses) -> ErrorCode | None:

        if requested_status.is_valid_status_transition(current_status=campaign.status):
            if requested_status == CampaignStatuses.ACTIVE and not campaign.is_activable():
                return ErrorCode.MISSING_CAMPAIGN_COMPONENTS
        else:
            return ErrorCode.INVALID_STATUS_REQUESTED

        return None

    async def _check_remaining_active_campaigns(
        self, db_session: "AsyncSession", campaign_slug: str, retailer: Retailer
    ) -> ErrorCode | None:
        error = False

        if not (active_campaigns := await crud.get_active_campaigns(db_session, retailer)):
            error = True

        active_campaign_slugs = [campaign.slug for campaign in active_campaigns]
        if set(active_campaign_slugs).issubset({campaign_slug}) and retailer.status is not RetailerStatuses.TEST:
            error = True

        return ErrorCode.INVALID_STATUS_REQUESTED if error else None

    async def _send_status_change_activities(
        self,
        *,
        campaign_status_activity_payload: dict,
        pr_delete_activity_payload: list[dict] | None,
        cancel_rewards_ap: list[dict] | None,
    ) -> None:
        await format_and_send_activity_in_background(
            self.background_tasks,
            activity_type=CampaignActivityType.CAMPAIGN,
            payload_formatter_fn=CampaignActivityType.get_campaign_status_change_activity_data,
            formatter_kwargs=campaign_status_activity_payload,
        )
        if pr_delete_activity_payload:
            await format_and_send_activity_in_background(
                self.background_tasks,
                activity_type=RewardsActivityType.REWARD_STATUS,
                payload_formatter_fn=RewardsActivityType.get_pending_reward_deleted_activity_data,
                formatter_kwargs=pr_delete_activity_payload,
            )
        if cancel_rewards_ap:
            await format_and_send_activity_in_background(
                self.background_tasks,
                activity_type=RewardsActivityType.REWARD_STATUS,
                payload_formatter_fn=RewardsActivityType.get_reward_status_activity_data,
                formatter_kwargs=cancel_rewards_ap,
            )

    async def _delete_pending_rewards_for_campaign(self, campaign: Campaign) -> list[dict]:
        """Executes crud method delete_pending_rewards_for_campaign and returns required data for this activity"""
        now = datetime.now(tz=timezone.utc)
        del_data = await delete_pending_rewards_for_campaign(self.db_session, retailer=self.retailer, campaign=campaign)
        return [
            {
                "activity_datetime": now,
                "retailer_slug": self.retailer.slug,
                "campaign_slug": campaign.slug,
                "account_holder_uuid": account_holder_uuid,
                "pending_reward_uuid": pending_reward_uuid,
            }
            for pending_reward_uuid, account_holder_uuid in del_data
        ]

    async def _cancel_issued_rewards_for_campaign(self, campaign: Campaign) -> list[dict]:
        """Executes crud method cancel_issued_rewards_for_campaign and returns required data for this activity"""

        rewards_cancelled_data = await cancel_issued_rewards_for_campaign(self.db_session, campaign=campaign)
        return [
            {
                "account_holder_uuid": account_holder_uuid,
                "retailer_slug": self.retailer.slug,
                "campaigns": [campaign.slug],
                "reason": "Reward cancelled due to campaign cancellation",
                "summary": f"{self.retailer.slug} Reward cancelled",
                "original_status": "issued",
                "new_status": "cancelled",
                "activity_datetime": cancelled_date,
                "activity_identifier": str(reward_uuid),
            }
            for cancelled_date, reward_uuid, account_holder_uuid in rewards_cancelled_data
        ]

    async def _handle_pending_rewards(
        self, campaign: "Campaign", pending_rewards_action: PendingRewardActions, requested_status: CampaignStatuses
    ) -> list[dict] | None:
        pr_delete_activity_payload: list[dict] | None = None
        if campaign.reward_rule.allocation_window > 0 and requested_status in (
            CampaignStatuses.ENDED,
            CampaignStatuses.CANCELLED,
        ):
            if pending_rewards_action == PendingRewardActions.CONVERT and requested_status == CampaignStatuses.ENDED:
                await convert_pending_rewards_placeholder()
            elif (
                pending_rewards_action == PendingRewardActions.REMOVE or requested_status == CampaignStatuses.CANCELLED
            ):
                pr_delete_activity_payload = await self._delete_pending_rewards_for_campaign(campaign)

        return pr_delete_activity_payload

    async def _handle_balances(self, campaign: "Campaign", requested_status: CampaignStatuses) -> list[dict] | None:
        if requested_status == CampaignStatuses.ACTIVE:
            await crud.create_campaign_balances(self.db_session, retailer=self.retailer, campaign=campaign)
        elif requested_status in (CampaignStatuses.ENDED, CampaignStatuses.CANCELLED):
            await crud.delete_campaign_balances(self.db_session, retailer=self.retailer, campaign=campaign)

        if requested_status == CampaignStatuses.CANCELLED:
            return await self._cancel_issued_rewards_for_campaign(campaign)

        return None

    async def _change_campaign_status(
        self, campaign: Campaign, requested_status: CampaignStatuses, sso_username: str
    ) -> dict:
        """Executes crud method campaign_status_change and returns required data for this activity"""

        original_status = campaign.status
        updated_campaign_values = await crud.campaign_status_change(
            db_session=self.db_session, campaign=campaign, requested_status=requested_status
        )
        return {
            "updated_at": updated_campaign_values.updated_at,
            "campaign_name": campaign.name,
            "campaign_slug": campaign.slug,
            "retailer_slug": self.retailer.slug,
            "original_status": original_status,
            "new_status": updated_campaign_values.status,
            "sso_username": sso_username,
        }

    async def handle_status_change(self, payload: CampaignsStatusChangeSchema) -> ServiceResult[dict, ServiceError]:

        requested_status = payload.requested_status
        campaign = await crud.get_campaign_by_slug(
            db_session=self.db_session, campaign_slug=payload.campaign_slug, retailer=self.retailer, load_rules=True
        )
        if not campaign:
            return ServiceResult(error=ServiceError(error_code=ErrorCode.NO_CAMPAIGN_FOUND))

        if error_code := await self._check_valid_campaign(campaign, requested_status):
            return ServiceResult(error=ServiceError(error_code=error_code))

        if requested_status in (CampaignStatuses.ENDED, CampaignStatuses.CANCELLED) and (
            error_code := await self._check_remaining_active_campaigns(
                db_session=self.db_session, campaign_slug=payload.campaign_slug, retailer=self.retailer
            )
        ):
            return ServiceResult(error=ServiceError(error_code=error_code))

        pr_delete_activity_payload = await self._handle_pending_rewards(
            campaign, payload.pending_rewards_action, requested_status
        )
        campaign_status_activity_payload = await self._change_campaign_status(
            campaign=campaign,
            requested_status=requested_status,
            sso_username=payload.activity_metadata.sso_username,
        )
        cancel_rewards_ap = await self._handle_balances(campaign, requested_status)

        await self.commit_db_changes()
        await self._send_status_change_activities(
            campaign_status_activity_payload=campaign_status_activity_payload,
            pr_delete_activity_payload=pr_delete_activity_payload,
            cancel_rewards_ap=cancel_rewards_ap,
        )
        return ServiceResult({})
