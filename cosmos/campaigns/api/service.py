from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from fastapi import status
from retry_tasks_lib.utils.asynchronous import async_create_many_tasks

from cosmos.accounts.api import crud as accounts_crud
from cosmos.accounts.config import account_settings
from cosmos.accounts.utils import get_accounts_queueable_task_ids
from cosmos.campaigns.activity.enums import ActivityType as CampaignActivityType
from cosmos.campaigns.api import crud
from cosmos.campaigns.enums import CampaignStatuses
from cosmos.core.api import crud as core_crud
from cosmos.core.api.service import Service, ServiceError, ServiceListError, ServiceResult
from cosmos.core.api.tasks import enqueue_many_tasks
from cosmos.core.error_codes import ErrorCode, ErrorCodeDetails
from cosmos.db.models import Campaign, Retailer
from cosmos.retailers.enums import RetailerStatuses
from cosmos.rewards.activity.enums import ActivityType as RewardsActivityType
from cosmos.rewards.activity.enums import IssuedRewardReasons
from cosmos.rewards.config import reward_settings
from cosmos.rewards.crud import (
    cancel_issued_rewards_for_campaign,
    delete_pending_rewards_for_campaign,
    transfer_pending_rewards,
)
from cosmos.rewards.enums import PendingRewardActions, PendingRewardMigrationActions

if TYPE_CHECKING:  # pragma: no cover
    from retry_tasks_lib.db.models import RetryTask
    from sqlalchemy.ext.asyncio import AsyncSession

    from cosmos.campaigns.api.schemas import CampaignsMigrationSchema, CampaignsStatusChangeSchema


@dataclass
class MigrationCampaigns:
    active: Campaign
    draft: Campaign


class CampaignService(Service):
    def __init__(self, db_session: "AsyncSession", retailer: "Retailer") -> None:
        super().__init__(db_session, retailer=retailer)
        self.tasks_to_enqueue_ids: set[int] = set()

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

    async def _issue_pending_rewards_for_campaign(self, campaign: Campaign) -> list["RetryTask"]:
        del_data = await delete_pending_rewards_for_campaign(db_session=self.db_session, campaign=campaign)
        return await async_create_many_tasks(
            self.db_session,
            task_type_name=reward_settings.REWARD_ISSUANCE_TASK_NAME,
            params_list=[
                {
                    "pending_reward_uuid": str(pending_reward_uuid),
                    "account_holder_id": account_holder_id,
                    "campaign_id": campaign.id,
                    "reward_config_id": campaign.reward_rule.reward_config_id,
                    "reason": IssuedRewardReasons.CONVERTED.name,
                }
                for _, pending_reward_uuid, count, account_holder_id, _ in del_data
                for _ in range(count)
            ],
        )

    async def _delete_pending_rewards_for_campaign(self, campaign: Campaign) -> None:
        """Executes crud method delete_pending_rewards_for_campaign and stores required data for this activity"""
        now = datetime.now(tz=UTC)
        del_data = await delete_pending_rewards_for_campaign(self.db_session, campaign=campaign)
        await self.store_activity(
            activity_type=RewardsActivityType.REWARD_STATUS,
            payload_formatter_fn=RewardsActivityType.get_pending_reward_deleted_activity_data,
            formatter_kwargs=[
                {
                    "activity_datetime": now,
                    "retailer_slug": self.retailer.slug,
                    "campaign_slug": campaign.slug,
                    "account_holder_uuid": account_holder_uuid,
                    "pending_reward_uuid": pending_reward_uuid,
                }
                for _, pending_reward_uuid, _, _, account_holder_uuid in del_data
            ],
        )

    async def _cancel_issued_rewards_for_campaign(self, campaign: Campaign) -> None:
        """Executes crud method cancel_issued_rewards_for_campaign and stores required data for this activity"""

        rewards_cancelled_data = await cancel_issued_rewards_for_campaign(self.db_session, campaign=campaign)
        await self.store_activity(
            activity_type=RewardsActivityType.REWARD_STATUS,
            payload_formatter_fn=RewardsActivityType.get_reward_status_activity_data,
            formatter_kwargs=[
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
            ],
        )

    async def _handle_pending_rewards(
        self, campaign: "Campaign", pending_rewards_action: PendingRewardActions, requested_status: CampaignStatuses
    ) -> None:
        if campaign.reward_rule.allocation_window and requested_status in (
            CampaignStatuses.ENDED,
            CampaignStatuses.CANCELLED,
        ):
            if pending_rewards_action == PendingRewardActions.CONVERT and requested_status == CampaignStatuses.ENDED:
                reward_issuance_tasks = await self._issue_pending_rewards_for_campaign(campaign=campaign)
                self.tasks_to_enqueue_ids |= {task.retry_task_id for task in reward_issuance_tasks}
            elif (
                pending_rewards_action == PendingRewardActions.REMOVE or requested_status == CampaignStatuses.CANCELLED
            ):
                await self._delete_pending_rewards_for_campaign(campaign)

    async def _update_enqueuable_task_ids_for_activation(self) -> None:
        pending_ah_ids = await accounts_crud.get_pending_account_holders(self.db_session, retailer=self.retailer)
        activation_tasks = await core_crud.get_waiting_retry_tasks_by_key_value_in(
            self.db_session,
            account_settings.ACCOUNT_HOLDER_ACTIVATION_TASK_NAME,
            "account_holder_id",
            [str(ah_id) for ah_id in pending_ah_ids],
        )
        tasks_to_enqueue_ids = get_accounts_queueable_task_ids(activation_tasks, set(pending_ah_ids))
        self.tasks_to_enqueue_ids |= set(tasks_to_enqueue_ids)

    async def _handle_actions_for_campaign_status_change(
        self, campaign: "Campaign", requested_status: CampaignStatuses
    ) -> None:
        if requested_status == CampaignStatuses.ACTIVE:
            await crud.create_campaign_balances(self.db_session, retailer=self.retailer, campaign=campaign)
            await self._update_enqueuable_task_ids_for_activation()

        elif requested_status in (CampaignStatuses.ENDED, CampaignStatuses.CANCELLED):
            await crud.delete_campaign_balances(self.db_session, retailer=self.retailer, campaign=campaign)

        if requested_status == CampaignStatuses.CANCELLED:
            await self._cancel_issued_rewards_for_campaign(campaign)

    async def _change_campaign_status(
        self, campaign: Campaign, requested_status: CampaignStatuses, sso_username: str
    ) -> None:
        """Executes crud method campaign_status_change and stores required data for this activity"""

        original_status = campaign.status
        updated_campaign_values = await crud.campaign_status_change(
            db_session=self.db_session, campaign=campaign, requested_status=requested_status
        )

        await self.store_activity(
            activity_type=CampaignActivityType.CAMPAIGN,
            payload_formatter_fn=CampaignActivityType.get_campaign_status_change_activity_data,
            formatter_kwargs={
                "updated_at": updated_campaign_values.updated_at,
                "campaign_name": campaign.name,
                "campaign_slug": campaign.slug,
                "retailer_slug": self.retailer.slug,
                "original_status": original_status,
                "new_status": updated_campaign_values.status,
                "sso_username": sso_username,
            },
        )

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

        await self._handle_pending_rewards(campaign, payload.pending_rewards_action, requested_status)
        await self._change_campaign_status(
            campaign=campaign,
            requested_status=requested_status,
            sso_username=payload.activity_metadata.sso_username,
        )
        await self._handle_actions_for_campaign_status_change(campaign, requested_status)

        await self.commit_db_changes()

        if self.tasks_to_enqueue_ids:
            await self.trigger_asyncio_task(enqueue_many_tasks(list(self.tasks_to_enqueue_ids)))

        await self.format_and_send_stored_activities()
        return ServiceResult({})

    async def _transfer_pending_rewards(self, *, from_campaign: Campaign, to_campaign: Campaign) -> None:
        updated_rewards = await transfer_pending_rewards(
            self.db_session, from_campaign=from_campaign, to_campaign=to_campaign
        )
        await self.store_activity(
            activity_type=RewardsActivityType.REWARD_STATUS,
            payload_formatter_fn=RewardsActivityType.get_pending_reward_transferred_activity_data,
            formatter_kwargs=[
                {
                    "retailer_slug": self.retailer.slug,
                    "from_campaign_slug": from_campaign.slug,
                    "to_campaign_slug": to_campaign.slug,
                    "account_holder_uuid": ah_uuid,
                    "activity_datetime": to_campaign.start_date,
                    "pending_reward_uuid": pr_uuid,
                }
                for _, pr_uuid, _, _, ah_uuid in updated_rewards
            ],
        )

    async def _transfer_balance(
        self, *, payload: "CampaignsMigrationSchema", from_campaign: Campaign, to_campaign: Campaign
    ) -> None:
        updated_balances = await crud.transfer_balance(
            self.db_session,
            from_campaign=from_campaign,
            to_campaign=to_campaign,
            threshold=payload.balance_action.qualifying_threshold,
            rate_percent=payload.balance_action.conversion_rate,
        )
        await self.store_activity(
            activity_type=CampaignActivityType.BALANCE_CHANGE,
            payload_formatter_fn=CampaignActivityType.get_balance_change_activity_data,
            formatter_kwargs=[
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
            ],
        )

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

        await self._change_campaign_status(
            campaigns.draft, CampaignStatuses.ACTIVE, payload.activity_metadata.sso_username
        )
        await crud.create_campaign_balances(self.db_session, retailer=self.retailer, campaign=campaigns.draft)

        if payload.balance_action.transfer:
            await crud.lock_balances_for_campaign(self.db_session, campaign=campaigns.active)
            await self._transfer_balance(payload=payload, from_campaign=campaigns.active, to_campaign=campaigns.draft)

        reward_issuance_tasks: list["RetryTask"] = []
        match payload.pending_rewards_action:
            case PendingRewardMigrationActions.TRANSFER:
                await self._transfer_pending_rewards(from_campaign=campaigns.active, to_campaign=campaigns.draft)
            case PendingRewardMigrationActions.CONVERT:
                reward_issuance_tasks = await self._issue_pending_rewards_for_campaign(campaign=campaigns.active)
            case PendingRewardMigrationActions.REMOVE:
                await self._delete_pending_rewards_for_campaign(campaigns.active)

        await self._change_campaign_status(
            campaigns.active, CampaignStatuses.ENDED, payload.activity_metadata.sso_username
        )
        await crud.delete_campaign_balances(self.db_session, retailer=self.retailer, campaign=campaigns.active)
        await self.commit_db_changes()
        if reward_issuance_tasks:
            await self.trigger_asyncio_task(enqueue_many_tasks([task.retry_task_id for task in reward_issuance_tasks]))
        await self.store_activity(
            activity_type=CampaignActivityType.CAMPAIGN_MIGRATION,
            payload_formatter_fn=CampaignActivityType.get_campaign_migration_activity_data,
            formatter_kwargs=[
                {
                    "retailer_slug": self.retailer.slug,
                    "from_campaign_slug": campaigns.active.slug,
                    "to_campaign_slug": campaigns.draft.slug,
                    "sso_username": payload.activity_metadata.sso_username,
                    "activity_datetime": datetime.now(tz=UTC),
                    "balance_conversion_rate": payload.balance_action.conversion_rate,
                    "qualify_threshold": payload.balance_action.qualifying_threshold,
                    "pending_rewards": payload.pending_rewards_action,
                    "transfer_balance_requested": payload.balance_action.transfer,
                }
            ],
        )
        await self.format_and_send_stored_activities()
        return ServiceResult({})
