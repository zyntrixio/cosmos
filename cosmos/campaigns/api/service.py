from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from fastapi import status

from cosmos.campaigns.activity.enums import ActivityType as CampaignActivityType
from cosmos.campaigns.api import crud
from cosmos.campaigns.enums import CampaignStatuses
from cosmos.core.activity.utils import format_and_send_activity_in_background
from cosmos.core.api.service import Service, ServiceError, ServiceListError, ServiceResult
from cosmos.core.error_codes import ErrorCode, ErrorCodeDetails
from cosmos.db.models import Campaign, Retailer
from cosmos.retailers.enums import RetailerStatuses
from cosmos.rewards.activity.enums import ActivityType as RewardsActivityType
from cosmos.rewards.crud import (
    cancel_issued_rewards_for_campaign,
    delete_pending_rewards_for_campaign,
    transfer_pending_rewards,
)
from cosmos.rewards.enums import PendingRewardActions, PendingRewardMigrationActions

if TYPE_CHECKING:  # pragma: no cover
    from sqlalchemy.ext.asyncio import AsyncSession

    from cosmos.campaigns.api.schemas import CampaignsMigrationSchema, CampaignsStatusChangeSchema


@dataclass
class MigrationCampaigns:
    active: Campaign
    draft: Campaign


async def convert_pending_rewards_placeholder() -> None:  # pragma: no cover
    # TODO: complete this logic once Carina reward agents have been implemented
    pass


class CampaignService(Service):
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
            activity_type=CampaignActivityType.CAMPAIGN,
            payload_formatter_fn=CampaignActivityType.get_campaign_status_change_activity_data,
            formatter_kwargs=campaign_status_activity_payload,
        )
        if pr_delete_activity_payload:
            await format_and_send_activity_in_background(
                activity_type=RewardsActivityType.REWARD_STATUS,
                payload_formatter_fn=RewardsActivityType.get_pending_reward_deleted_activity_data,
                formatter_kwargs=pr_delete_activity_payload,
            )
        if cancel_rewards_ap:
            await format_and_send_activity_in_background(
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

    async def handle_status_change(self, payload: "CampaignsStatusChangeSchema") -> ServiceResult[dict, ServiceError]:

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

    async def _send_migration_activities(
        self,
        *,
        activate_draft_ap: dict,
        ending_active_ap: dict,
        balance_transfer_ap: list[dict] | None,
        pr_delete_ap: list[dict] | None,
        pr_transfer_ap: list[dict] | None,
    ) -> None:

        for activity_payload in (activate_draft_ap, ending_active_ap):
            await format_and_send_activity_in_background(
                activity_type=CampaignActivityType.CAMPAIGN,
                payload_formatter_fn=CampaignActivityType.get_campaign_status_change_activity_data,
                formatter_kwargs=activity_payload,
            )

        if balance_transfer_ap:
            await format_and_send_activity_in_background(
                activity_type=CampaignActivityType.BALANCE_CHANGE,
                payload_formatter_fn=CampaignActivityType.get_balance_change_activity_data,
                formatter_kwargs=balance_transfer_ap,
            )
        if pr_delete_ap:
            await format_and_send_activity_in_background(
                activity_type=RewardsActivityType.REWARD_STATUS,
                payload_formatter_fn=RewardsActivityType.get_pending_reward_deleted_activity_data,
                formatter_kwargs=pr_delete_ap,
            )
        if pr_transfer_ap:
            await format_and_send_activity_in_background(
                activity_type=RewardsActivityType.REWARD_STATUS,
                payload_formatter_fn=RewardsActivityType.get_pending_reward_transferred_activity_data,
                formatter_kwargs=pr_transfer_ap,
            )

    async def _transfer_pending_rewards(self, *, from_campaign: Campaign, to_campaign: Campaign) -> list[dict]:
        updated_rewards = await transfer_pending_rewards(
            self.db_session, from_campaign=from_campaign, to_campaign=to_campaign
        )
        return [
            {
                "retailer_slug": self.retailer.slug,
                "from_campaign_slug": from_campaign.slug,
                "to_campaign_slug": to_campaign.slug,
                "account_holder_uuid": ah_uuid,
                "activity_datetime": to_campaign.start_date,
                "pending_reward_uuid": pr_uuid,
            }
            for pr_uuid, ah_uuid in updated_rewards
        ]

    async def _transfer_balance(
        self, *, payload: "CampaignsMigrationSchema", from_campaign: Campaign, to_campaign: Campaign
    ) -> list[dict]:

        updated_balances = await crud.transfer_balance(
            self.db_session,
            from_campaign=from_campaign,
            to_campaign=to_campaign,
            threshold=payload.balance_action.qualifying_threshold,
            rate_percent=payload.balance_action.conversion_rate,
        )
        return [
            {
                "retailer_slug": self.retailer.slug,
                "from_campaign_slug": from_campaign.slug,
                "to_campaign_slug": to_campaign.slug,
                "account_holder_uuid": ah_uuid,
                "activity_datetime": to_campaign.start_date,
                "new_balance": balance,
                "loyalty_type": to_campaign.loyalty_type,
            }
            for ah_uuid, balance in updated_balances
        ]

    async def _fetch_campaigns(
        self, from_campaign_slug: str, to_campaign_slug: str
    ) -> MigrationCampaigns | ServiceListError:
        from_campaign = await crud.get_campaign_by_slug(
            db_session=self.db_session,
            campaign_slug=from_campaign_slug,
            retailer=self.retailer,
            load_rules=True,
            lock_row=False,
        )
        to_campaign = await crud.get_campaign_by_slug(
            db_session=self.db_session,
            campaign_slug=to_campaign_slug,
            retailer=self.retailer,
            load_rules=True,
            lock_row=False,
        )
        if from_campaign and to_campaign:
            return MigrationCampaigns(active=from_campaign, draft=to_campaign)

        campaigns_not_found: list[str] = []
        if not from_campaign:
            campaigns_not_found.append(from_campaign_slug)

        if not to_campaign:
            campaigns_not_found.append(to_campaign_slug)

        return ServiceListError(
            error_details=[ErrorCodeDetails.NO_CAMPAIGN_FOUND.set_optional_fields(campaigns=campaigns_not_found)],
            status_code=status.HTTP_404_NOT_FOUND,
        )

    async def _check_campaigns_for_migration(
        self, from_campaign: Campaign, to_campaign: Campaign
    ) -> ServiceError | ServiceListError | None:

        #  this is already checked in the Admin panel action and should not happen
        if from_campaign.loyalty_type != to_campaign.loyalty_type:
            return ServiceError(error_code=ErrorCode.INVALID_REQUEST)

        invalid_campaigns: dict = {
            ErrorCodeDetails.MISSING_CAMPAIGN_COMPONENTS: [],
            ErrorCodeDetails.INVALID_STATUS_REQUESTED: [],
        }

        if not CampaignStatuses.ENDED.is_valid_status_transition(
            current_status=from_campaign.status
        ) or await self._check_remaining_active_campaigns(
            db_session=self.db_session,
            campaign_slug=from_campaign.slug,
            retailer=self.retailer,
        ):
            invalid_campaigns[ErrorCodeDetails.INVALID_STATUS_REQUESTED].append(from_campaign.slug)

        if not CampaignStatuses.ACTIVE.is_valid_status_transition(current_status=to_campaign.status):
            invalid_campaigns[ErrorCodeDetails.INVALID_STATUS_REQUESTED].append(to_campaign.slug)
        if not to_campaign.is_activable():
            invalid_campaigns[ErrorCodeDetails.MISSING_CAMPAIGN_COMPONENTS].append(to_campaign.slug)

        if error_details := [
            ErrorCodeDetails(k).set_optional_fields(campaigns=v) for k, v in invalid_campaigns.items() if v
        ]:
            return ServiceListError(error_details=error_details, status_code=status.HTTP_409_CONFLICT)

        return None

    async def handle_migration(
        self, payload: "CampaignsMigrationSchema"
    ) -> ServiceResult[dict, ServiceError | ServiceListError]:

        campaigns = await self._fetch_campaigns(
            from_campaign_slug=payload.from_campaign, to_campaign_slug=payload.to_campaign
        )
        if isinstance(campaigns, ServiceListError):
            return ServiceResult(error=campaigns)

        if error := await self._check_campaigns_for_migration(
            from_campaign=campaigns.active, to_campaign=campaigns.draft
        ):
            return ServiceResult(error=error)

        activate_draft_ap = await self._change_campaign_status(
            campaigns.draft, CampaignStatuses.ACTIVE, payload.activity_metadata.sso_username
        )
        await crud.create_campaign_balances(self.db_session, retailer=self.retailer, campaign=campaigns.draft)

        balance_transfer_ap: list[dict] | None = None
        if payload.balance_action.transfer:
            await crud.lock_balances_for_campaign(self.db_session, campaign=campaigns.active)
            balance_transfer_ap = await self._transfer_balance(
                payload=payload, from_campaign=campaigns.active, to_campaign=campaigns.draft
            )

        pr_delete_ap: list[dict] | None = None
        pr_transfer_ap: list[dict] | None = None
        match payload.pending_rewards_action:  # noqa: E999
            case PendingRewardMigrationActions.TRANSFER:
                pr_transfer_ap = await self._transfer_pending_rewards(
                    from_campaign=campaigns.active, to_campaign=campaigns.draft
                )
            case PendingRewardMigrationActions.CONVERT:
                await convert_pending_rewards_placeholder()
            case PendingRewardMigrationActions.REMOVE:
                pr_delete_ap = await self._delete_pending_rewards_for_campaign(campaigns.active)

        ending_active_ap = await self._change_campaign_status(
            campaigns.active, CampaignStatuses.ENDED, payload.activity_metadata.sso_username
        )
        await crud.delete_campaign_balances(self.db_session, retailer=self.retailer, campaign=campaigns.active)
        await self.commit_db_changes()
        await self._send_migration_activities(
            activate_draft_ap=activate_draft_ap,
            ending_active_ap=ending_active_ap,
            balance_transfer_ap=balance_transfer_ap,
            pr_delete_ap=pr_delete_ap,
            pr_transfer_ap=pr_transfer_ap,
        )
        return ServiceResult({})
