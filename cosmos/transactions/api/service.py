import logging
import uuid

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from pydantic import BaseModel, NonNegativeInt, PositiveInt

from cosmos.accounts.api import crud as accounts_crud
from cosmos.accounts.api.schemas.account_holder import AccountHolderStatuses
from cosmos.core.api.crud import commit
from cosmos.core.api.service import Service, ServiceException, ServiceResult
from cosmos.core.error_codes import ErrorCode
from cosmos.db.models import Campaign, CampaignBalance, EarnRule, LoyaltyTypes, PendingReward, Retailer, Transaction
from cosmos.transactions.api import crud
from cosmos.transactions.api.schemas import CreateTransactionSchema

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger("transaction-service")


class RewardUpdateDataSchema(BaseModel):
    new_total_cost_to_user: int
    original_total_cost_to_user: int


class TotalCostToUserDataSchema(RewardUpdateDataSchema):
    pending_reward_id: int
    pending_reward_uuid: uuid.UUID


@dataclass
class AdjustmentAmount:
    type: LoyaltyTypes
    amount: int
    threshold: int
    accepted: bool


def _get_transaction_response(adjustments: list, is_refund: bool) -> str:
    if adjustments:
        if is_refund:
            response = "Refund accepted"
        else:
            response = "Awarded"

    else:
        if is_refund:
            response = "Refunds not accepted"
        else:
            response = "Threshold not met"
    return response


class TransactionService(Service):
    def _adjustment_amount_for_earn_rule(
        self, tx_amount: int, loyalty_type: LoyaltyTypes, earn_rule: EarnRule, allocation_window: int
    ) -> int | None:

        if loyalty_type == LoyaltyTypes.ACCUMULATOR:
            adjustment_amount = self._calculate_amount_for_accumulator(tx_amount, earn_rule, allocation_window)

        elif loyalty_type == LoyaltyTypes.STAMPS and tx_amount >= earn_rule.threshold:
            adjustment_amount = earn_rule.increment * earn_rule.increment_multiplier

        return adjustment_amount

    def _calculate_amount_for_accumulator(
        self, tx_amount: int, earn_rule: EarnRule, allocation_window: int
    ) -> int | None:
        is_acceptable_refund = bool(tx_amount < 0 and allocation_window)
        adjustment_amount = None

        if earn_rule.max_amount and abs(tx_amount) > earn_rule.max_amount:
            if is_acceptable_refund:
                adjustment_amount = -(earn_rule.max_amount)
            elif tx_amount > 0:
                adjustment_amount = earn_rule.max_amount
        elif is_acceptable_refund or tx_amount >= earn_rule.threshold:
            # FIXME - increment multiplier could be 1.25 e.g. 399 * 1.25 = 498.75. What do we do?
            # This will round to the nearest whole number
            adjustment_amount = round(tx_amount * earn_rule.increment_multiplier)

        return adjustment_amount

    def _rewards_achieved(self, campaign: Campaign, new_balance: int, adjustment_amount: int) -> tuple[int, bool]:
        reward_rule = campaign.reward_rule
        n_reward_achieved = new_balance // reward_rule.reward_goal
        trc_reached = False

        if reward_rule.reward_cap and (
            n_reward_achieved > reward_rule.reward_cap.value
            or adjustment_amount > reward_rule.reward_cap * reward_rule.reward_goal
        ):
            n_reward_achieved = reward_rule.reward_cap.value
            trc_reached = True

        return n_reward_achieved, trc_reached

    # async def _emit_events(self, account_holder, total_costs, deleted_count_by_uuid, amount_not_recouped):
    #     if amount_not_recouped > 0:
    #         activity_payload = ActivityType.get_refund_not_recouped_activity_data(
    #             account_holder_uuid=account_holder.account_holder_uuid,
    #             retailer_slug=self.retailer.slug,
    #             adjustment=adjustment,
    #             amount_recouped=abs(adjustment) - amount_not_recouped,
    #             amount_not_recouped=amount_not_recouped,
    #             campaigns=[campaign_slug],
    #             activity_datetime=activity_metadata["transaction_datetime"],
    #             transaction_id=transaction_id,
    #         )
    #         asyncio_create_task(
    #             async_send_activity(activity_payload, routing_key=ActivityType.REFUND_NOT_RECOUPED.value)
    #         )
    #     if current_balance_obj.balance != original_balance:
    #         activity_payload = ActivityType.get_balance_change_activity_data(
    #             account_holder_uuid=account_holder.account_holder_uuid,
    #             retailer_slug=retailer.slug,
    #             summary=f"{retailer.name} {campaign_slug} Balance {adjustment}",
    #             original_balance=original_balance,
    #             new_balance=current_balance_obj.balance,
    #             campaigns=[campaign_slug],
    #             activity_datetime=activity_metadata.get("transaction_datetime", datetime.now(tz=timezone.utc)),
    #             reason=activity_metadata["reason"],
    #         )
    #         asyncio_create_task(
    #             async_send_activity(activity_payload, routing_key=ActivityType.BALANCE_CHANGE.value)
    #         )
    #     for pending_reward_uuid, count in deleted_count_by_uuid.items():
    #         # Asynchronously send reward activity for all pending rewards deleted
    #         activity_payload = ActivityType.get_reward_status_activity_data(
    #             account_holder_uuid=account_holder.account_holder_uuid,
    #             retailer_slug=retailer.slug,
    #             summary=f"{retailer.slug} Pending reward deleted for {campaign_slug}",
    #             reason="Pending Reward removed due to refund",
    #             original_status="pending",
    #             new_status="deleted",
    #             campaigns=[campaign_slug],
    #             activity_datetime=activity_metadata["transaction_datetime"],
    #             activity_identifier=str(pending_reward_uuid),
    #             count=count,
    #         )
    #         asyncio_create_task(async_send_activity(activity_payload, routing_key=ActivityType.REWARD_STATUS.value))
    #     if total_costs:
    #         await _process_total_costs(
    #             db_session=db_session,
    #             total_costs=total_costs,
    #             deleted_count_by_uuid=deleted_count_by_uuid,
    #             account_holder_uuid=account_holder.account_holder_uuid,
    #             retailer_slug=retailer.slug,
    #             campaign_slug=campaign_slug,
    #         )

    async def _adjust_balance(
        self, campaign: Campaign, campaign_balance: CampaignBalance, transaction: Transaction
    ) -> int | None:
        adjustment = self._adjustment_amount_for_earn_rule(
            transaction.amount, campaign.loyalty_type, campaign.earn_rule, campaign.reward_rule.allocation_window
        )

        if adjustment is not None:
            total_costs: list[TotalCostToUserDataSchema]
            if adjustment < 0:
                pending_rewards = await crud.get_pending_rewards_for_update(
                    self.db_session,
                    account_holder_id=campaign_balance.account_holder_id,
                    campaign_id=campaign_balance.campaign_id,
                )
                (
                    campaign_balance.balance,
                    deleted_count_by_uuid,
                    amount_not_recouped,
                    total_costs,
                ) = await self._process_refund(
                    shortfall=abs(adjustment), current_balance=campaign_balance.balance, pending_rewards=pending_rewards
                )
            else:
                campaign_balance.balance += adjustment
                deleted_count_by_uuid = {}
                total_costs = []
                amount_not_recouped = 0

            # try:
            #     await self._emit_events(total_costs, deleted_count_by_uuid, amount_not_recouped)
            # except Exception as exc:
            #     pass
        return adjustment

    async def _process_refund(
        self,
        *,
        shortfall: NonNegativeInt,
        current_balance: PositiveInt,
        pending_rewards: list[PendingReward],
    ) -> tuple[int, dict, NonNegativeInt, list[TotalCostToUserDataSchema]]:
        """
        Shortfall absorption logic:
        full [documentation](https://hellobink.atlassian.net/wiki/spaces/BPL/pages/\
    3235184690/Examples+of+Refund+Pending+Reward+Scenario+s#Flow-Diagram)

        1 - use a single prr with a slush big enough to cover the whole amount,
        if more than one is found, use the newest. If this does't work we try steps 2 to 4 sequentially to reduce
        the shortfall until the shortfall reaches zero or we reach step 5.

        2 - use prrs' collective slush.

        3 - use existing balance.

        4 - use prrs' collective total_value.

        5 - balance is zero and we have no more prrs we stop here.


        NB: this is meant to be called from within a async_run_query that commits the changes.
        """

        deleted_count_by_uuid: dict = {}
        total_costs: list[TotalCostToUserDataSchema] = []

        # try to use a single prr's slush to absorb the shortfall
        if prr_with_slush_ge_shortfall := next((prr for prr in pending_rewards if prr.slush >= shortfall), None):
            original_total_cost_to_user = prr_with_slush_ge_shortfall.total_cost_to_user
            prr_with_slush_ge_shortfall.slush -= shortfall
            total_costs.append(
                TotalCostToUserDataSchema(
                    new_total_cost_to_user=prr_with_slush_ge_shortfall.total_cost_to_user,
                    original_total_cost_to_user=original_total_cost_to_user,
                    pending_reward_id=prr_with_slush_ge_shortfall.id,
                    pending_reward_uuid=prr_with_slush_ge_shortfall.pending_reward_uuid,
                )
            )
            shortfall = 0
            return current_balance, deleted_count_by_uuid, shortfall, total_costs

        # try to use collective slush of all available prrs to absorb shortfall
        for pending_reward in [prr for prr in pending_rewards if prr.slush > 0]:

            if pending_reward.slush >= shortfall:
                original_total_cost_to_user = pending_reward.total_cost_to_user
                pending_reward.total_cost_to_user -= shortfall
                total_costs.append(
                    TotalCostToUserDataSchema(
                        new_total_cost_to_user=pending_reward.total_cost_to_user,
                        original_total_cost_to_user=original_total_cost_to_user,
                        pending_reward_id=pending_reward.id,
                        pending_reward_uuid=pending_reward.pending_reward_uuid,
                    )
                )
                shortfall = 0
                return current_balance, deleted_count_by_uuid, shortfall, total_costs

            shortfall -= pending_reward.slush
            original_total_cost_to_user = pending_reward.total_cost_to_user
            pending_reward.total_cost_to_user -= shortfall
            total_costs.append(
                TotalCostToUserDataSchema(
                    new_total_cost_to_user=pending_reward.total_cost_to_user,
                    original_total_cost_to_user=original_total_cost_to_user,
                    pending_reward_id=pending_reward.id,
                    pending_reward_uuid=pending_reward.pending_reward_uuid,
                )
            )
            pending_reward.slush = 0

        # try to use existing balance to absorb remaining shortfall
        if current_balance >= shortfall:
            current_balance -= shortfall
            shortfall = 0
            return current_balance, deleted_count_by_uuid, shortfall, total_costs

        shortfall -= current_balance
        current_balance = 0

        # try to use prr total_values to absorb remaining shortfall
        current_balance, deleted_count_by_uuid, shortfall = await self._use_pending_rewards_to_absorb_balance_shortfall(
            pending_rewards, current_balance, shortfall, deleted_count_by_uuid
        )

        return current_balance, deleted_count_by_uuid, shortfall, total_costs

    async def _use_pending_rewards_to_absorb_balance_shortfall(
        self,
        pending_rewards: list[PendingReward],
        current_balance: PositiveInt,
        shortfall: PositiveInt,
        deleted_count_by_uuid: dict,
    ) -> tuple[PositiveInt, dict, int]:
        """NB: this is meant to be called from within a async_run_query that commits the changes."""

        for pending_reward in pending_rewards:
            if shortfall == 0:
                break

            if (remainder := pending_reward.total_value - shortfall) > 0:
                current_balance = remainder % pending_reward.value
                shortfall = 0

                if (new_count := remainder // pending_reward.value) >= 1:
                    deleted_count_by_uuid[str(pending_reward.pending_reward_uuid)] = pending_reward.count - new_count
                    pending_reward.count = new_count
                    pending_reward.slush = 0
                else:
                    deleted_count_by_uuid[str(pending_reward.pending_reward_uuid)] = pending_reward.count
                    await crud.delete_pending_reward(self.db_session, pending_reward)

            else:
                shortfall -= pending_reward.total_value
                deleted_count_by_uuid[str(pending_reward.pending_reward_uuid)] = pending_reward.count
                await crud.delete_pending_reward(self.db_session, pending_reward)

        return current_balance, deleted_count_by_uuid, shortfall

    async def _allocate_reward(self) -> None:
        pass

    async def _process_rewards(
        self, transaction: Transaction, campaign: Campaign, campaign_balance_amount: int, adjustment: int
    ) -> None:
        log_suffix = f"(tx_id: {transaction.transaction_id})"
        rewards_achieved_n, trc_reached = self._rewards_achieved(campaign, campaign_balance_amount, adjustment)
        if rewards_achieved_n > 0:
            if trc_reached:
                tot_cost_to_user = adjustment
                logger.info("Transaction reward cap '%s' reached %s", campaign.reward_rule.reward_cap, log_suffix)
                post_msg = "Transaction reward cap reached, decreasing balance by original adjustment amount (%s) %s"

            else:
                tot_cost_to_user = rewards_achieved_n * campaign.reward_rule.reward_goal
                logger.info(
                    "Reward goal (%d) met %d time%s %s",
                    campaign.reward_rule.reward_goal,
                    rewards_achieved_n,
                    "s" if rewards_achieved_n > 1 else "",
                    log_suffix,
                )
                post_msg = "Decreasing balance by total rewards value (%s) %s"

            if campaign.reward_rule.allocation_window > 0:
                await crud.create_pending_reward(
                    self.db_session,
                    account_holder_id=transaction.account_holder_id,
                    campaign_id=campaign.id,
                    conversion_date=(
                        datetime.now(tz=timezone.utc) + timedelta(days=campaign.reward_rule.allocation_window)
                    ).date(),
                    value=campaign.reward_rule.reward_goal,
                    count=rewards_achieved_n,
                    total_cost_to_user=tot_cost_to_user,
                )
            else:
                await self._allocate_reward()

    async def handle_incoming_transaction(self, request_payload: CreateTransactionSchema) -> ServiceResult:
        "Main handler for incoming transactions"

        account_holder = await accounts_crud.get_account_holder(
            self.db_session, retailer_id=self.retailer.id, account_holder_uuid=request_payload.account_holder_uuid
        )
        if not account_holder:
            return ServiceResult(ServiceException(error_code=ErrorCode.USER_NOT_FOUND))
        if account_holder.status != AccountHolderStatuses.ACTIVE:
            return ServiceResult(ServiceException(error_code=ErrorCode.USER_NOT_ACTIVE))

        transaction = await crud.create_transaction(
            self.db_session,
            account_holder_id=account_holder.id,
            retailer_id=self.retailer.id,
            transaction_data=request_payload,
        )
        if not transaction.processed:
            await commit(self.db_session)
            return ServiceResult(ServiceException(error_code=ErrorCode.DUPLICATE_TRANSACTION))

        campaigns = list(
            filter(
                lambda campaign: campaign.start_date <= transaction.datetime
                and (campaign.end_date is None or campaign.end_date > transaction.datetime),
                self.retailer.campaigns,
            )
        )
        if not campaigns:
            return ServiceResult(ServiceException(error_code=ErrorCode.NO_ACTIVE_CAMPAIGNS))

        locked_balances = await crud.get_balances_for_update(
            self.db_session, account_holder_id=account_holder.id, campaigns=campaigns
        )
        balances_by_campaign_id = {balance.campaign_id: balance for balance in locked_balances}

        adjustments = []
        for campaign in campaigns:
            campaign_balance = balances_by_campaign_id[campaign.id]
            adjustment = await self._adjust_balance(
                campaign=campaign, campaign_balance=campaign_balance, transaction=transaction
            )
            await crud.associate_campaign_to_transaction(self.db_session, campaign.id, transaction.id, adjustment)
            if adjustment:
                adjustments.append(adjustment)
                if adjustment > 0:
                    await self._process_rewards(
                        transaction=transaction,
                        campaign=campaign,
                        campaign_balance_amount=campaign_balance.balance,
                        adjustment=adjustment,
                    )

        await commit(self.db_session)
        return ServiceResult(_get_transaction_response(adjustments, transaction.amount < 0))
