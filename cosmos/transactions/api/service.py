import logging
import uuid

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from pydantic import BaseModel, NonNegativeInt, PositiveInt
from retry_tasks_lib.utils.asynchronous import async_create_many_tasks

from cosmos.accounts.activity.enums import ActivityType as AccountActivityType
from cosmos.accounts.api import crud as accounts_crud
from cosmos.accounts.api.schemas.account_holder import AccountHolderStatuses
from cosmos.campaigns.enums import CampaignStatuses
from cosmos.core.api.service import Service, ServiceError, ServiceResult
from cosmos.core.api.tasks import enqueue_many_tasks
from cosmos.core.error_codes import ErrorCode
from cosmos.core.utils import pence_integer_to_currency_string, raw_stamp_value_to_string
from cosmos.db.models import Campaign, CampaignBalance, EarnRule, LoyaltyTypes, PendingReward, Transaction
from cosmos.retailers.enums import RetailerStatuses
from cosmos.rewards.activity.enums import ActivityType as RewardsActivityType
from cosmos.rewards.activity.enums import IssuedRewardReasons
from cosmos.rewards.config import reward_settings
from cosmos.transactions.activity.enums import ActivityType as TransactionActivityType
from cosmos.transactions.api import crud
from cosmos.transactions.api.schemas import CreateTransactionSchema

if TYPE_CHECKING:
    from typing import TypeVar

    from retry_tasks_lib.db.models import RetryTask

    from cosmos.db.models import AccountHolder

    ServiceResultType = TypeVar("ServiceResultType")

logger = logging.getLogger("transaction-service")


class RewardUpdateDataSchema(BaseModel):
    new_total_cost_to_user: int
    original_total_cost_to_user: int


class TotalCostToUserDataSchema(RewardUpdateDataSchema):
    pending_reward_uuid: uuid.UUID
    pending_reward_updated_at: datetime


@dataclass
class AdjustmentAmount:
    loyalty_type: LoyaltyTypes
    amount: int
    threshold: int
    accepted: bool


class TransactionService(Service):
    @staticmethod
    def _get_transaction_response(accepted_adjustments: bool, is_refund: bool) -> str:
        if accepted_adjustments:
            return "Refund accepted" if is_refund else "Awarded"

        return "Refunds not accepted" if is_refund else "Threshold not met"

    def _adjustment_amount_for_earn_rule(
        self, tx_amount: int, loyalty_type: LoyaltyTypes, earn_rule: EarnRule, allocation_window: int | None
    ) -> int | None:

        if loyalty_type == LoyaltyTypes.ACCUMULATOR:
            return self._calculate_amount_for_accumulator(tx_amount, earn_rule, allocation_window)

        if loyalty_type == LoyaltyTypes.STAMPS:
            return (
                int(earn_rule.increment * earn_rule.increment_multiplier) if tx_amount >= earn_rule.threshold else None
            )

        raise ValueError(f"Invalid Loyalty Type {loyalty_type}")

    def _calculate_amount_for_accumulator(
        self, tx_amount: int, earn_rule: EarnRule, allocation_window: int | None
    ) -> int | None:
        is_acceptable_refund = bool(tx_amount < 0 and allocation_window)
        adjustment_amount: int | None = None

        if earn_rule.max_amount and abs(tx_amount) > earn_rule.max_amount:
            if is_acceptable_refund:
                adjustment_amount = -(earn_rule.max_amount)
            elif tx_amount > 0:
                adjustment_amount = earn_rule.max_amount
        elif is_acceptable_refund or tx_amount >= earn_rule.threshold:
            # FIXME - increment multiplier could be 1.25 e.g. 399 * 1.25 = 498.75. What do we do?
            # This will truncate the decimals.
            adjustment_amount = int(tx_amount * earn_rule.increment_multiplier)

        return adjustment_amount

    def _rewards_achieved(self, campaign: Campaign, new_balance: int, adjustment_amount: int) -> tuple[int, bool]:
        reward_rule = campaign.reward_rule
        n_reward_achieved = new_balance // reward_rule.reward_goal
        trc_reached = False

        if reward_rule.reward_cap and (
            n_reward_achieved > reward_rule.reward_cap
            or adjustment_amount > reward_rule.reward_cap * reward_rule.reward_goal
        ):
            n_reward_achieved = reward_rule.reward_cap
            trc_reached = True

        return n_reward_achieved, trc_reached

    async def _generate_balance_adjustment_activities(
        self,
        *,
        amount_not_recouped: int,
        adjustment_amount: int,
        new_balance: int,
        original_balance: int,
        campaign: Campaign,
        transaction: Transaction,
        account_holder_uuid: uuid.UUID,
        reason_prefix: str,
        deleted_count_by_uuid: dict,
        total_costs: list[TotalCostToUserDataSchema],
    ) -> None:
        if amount_not_recouped > 0:
            await self.store_activity(
                activity_type=TransactionActivityType.REFUND_NOT_RECOUPED,
                payload_formatter_fn=TransactionActivityType.get_refund_not_recouped_activity_data,
                formatter_kwargs={
                    "account_holder_uuid": account_holder_uuid,
                    "retailer": self.retailer,
                    "adjustment": adjustment_amount,
                    "amount_recouped": abs(amount_not_recouped) - amount_not_recouped,
                    "amount_not_recouped": amount_not_recouped,
                    "campaigns": [campaign.slug],
                    "activity_datetime": transaction.datetime,
                    "transaction_id": transaction.transaction_id,
                },
            )

        if new_balance != original_balance:
            await self.store_activity(
                activity_type=AccountActivityType.BALANCE_CHANGE,
                payload_formatter_fn=AccountActivityType.get_balance_change_activity_data,
                formatter_kwargs={
                    "account_holder_uuid": account_holder_uuid,
                    "retailer_slug": self.retailer.slug,
                    "summary": await self._get_summary_for_balance_change(campaign, adjustment_amount),
                    "original_balance": original_balance,
                    "new_balance": new_balance,
                    "campaigns": [campaign.slug],
                    "activity_datetime": transaction.datetime,
                    "reason": f"{reason_prefix} transaction id: {transaction.transaction_id}",
                },
            )

        if deleted_count_by_uuid:
            await self.store_activity(
                activity_type=RewardsActivityType.REWARD_STATUS,
                payload_formatter_fn=RewardsActivityType.get_reward_status_activity_data,
                formatter_kwargs=[
                    {
                        "account_holder_uuid": account_holder_uuid,
                        "retailer_slug": self.retailer.slug,
                        "summary": f"{self.retailer.name} Pending reward deleted for {campaign.name}",
                        "reason": "Pending Reward removed due to refund",
                        "original_status": "pending",
                        "new_status": "deleted",
                        "campaigns": [campaign.slug],
                        "activity_datetime": transaction.datetime,
                        "activity_identifier": str(pending_reward_uuid),
                        "count": count,
                    }
                    for pending_reward_uuid, count in deleted_count_by_uuid.items()
                ],
            )

        if updated_rewards_total_costs := [
            total_cost for total_cost in total_costs if str(total_cost.pending_reward_uuid) not in deleted_count_by_uuid
        ]:
            await self.store_activity(
                activity_type=RewardsActivityType.REWARD_UPDATE,
                payload_formatter_fn=RewardsActivityType.get_reward_update_activity_data,
                formatter_kwargs=[
                    {
                        "account_holder_uuid": account_holder_uuid,
                        "retailer_slug": self.retailer.slug,
                        "summary": r"Pending Reward Record's total cost to user updated",
                        "campaigns": [campaign.slug],
                        "reason": "Pending Reward updated due to refund",
                        "activity_datetime": total_cost.pending_reward_updated_at,
                        "activity_identifier": str(total_cost.pending_reward_uuid),
                        "reward_update_data": {
                            "new_total_cost_to_user": total_cost.new_total_cost_to_user,
                            "original_total_cost_to_user": total_cost.original_total_cost_to_user,
                        },
                    }
                    for total_cost in updated_rewards_total_costs
                ],
            )

    async def _adjust_balance(
        self,
        campaign: Campaign,
        campaign_balance: CampaignBalance,
        transaction: Transaction,
        account_holder_uuid: uuid.UUID,
    ) -> AdjustmentAmount:
        adjustment_amount = self._adjustment_amount_for_earn_rule(
            transaction.amount, campaign.loyalty_type, campaign.earn_rule, campaign.reward_rule.allocation_window
        )

        if not adjustment_amount:
            return AdjustmentAmount(
                loyalty_type=campaign.loyalty_type,
                amount=0,
                threshold=campaign.earn_rule.threshold,
                accepted=False,
            )

        original_balance = campaign_balance.balance
        deleted_count_by_uuid: dict = {}
        amount_not_recouped = 0
        total_costs: list[TotalCostToUserDataSchema] = []
        if adjustment_amount < 0:
            reason_prefix = "Refund"
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
                shortfall=abs(adjustment_amount),
                current_balance=campaign_balance.balance,
                pending_rewards=pending_rewards,
            )
        else:
            reason_prefix = "Purchase"
            campaign_balance.balance += adjustment_amount

        await self._generate_balance_adjustment_activities(
            amount_not_recouped=amount_not_recouped,
            adjustment_amount=adjustment_amount,
            new_balance=campaign_balance.balance,
            original_balance=original_balance,
            campaign=campaign,
            transaction=transaction,
            account_holder_uuid=account_holder_uuid,
            reason_prefix=reason_prefix,
            deleted_count_by_uuid=deleted_count_by_uuid,
            total_costs=total_costs,
        )

        return AdjustmentAmount(
            loyalty_type=campaign.loyalty_type,
            amount=adjustment_amount or 0,
            threshold=campaign.earn_rule.threshold,
            accepted=adjustment_amount is not None,
        )

    async def _get_summary_for_balance_change(self, campaign: Campaign, adjustment: int) -> str:
        tx_type = "+" if adjustment > 0 else ""
        if campaign.loyalty_type == LoyaltyTypes.ACCUMULATOR:
            return (
                f"{self.retailer.name} - {campaign.name}: "
                f"{tx_type}{pence_integer_to_currency_string(adjustment, 'GBP')}"
            )

        return f"{self.retailer.name} - {campaign.name}: {tx_type}{raw_stamp_value_to_string(adjustment)}"

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

        # asyncpg cant convert timezone aware to naive, remove this once we move to psycopg3
        pending_rewards_updated_at = datetime.now(tz=UTC).replace(tzinfo=None)
        deleted_count_by_uuid: dict = {}
        total_costs: list[TotalCostToUserDataSchema] = []

        # try to use a single prr's slush to absorb the shortfall
        if prr_with_slush_ge_shortfall := next((prr for prr in pending_rewards if prr.slush >= shortfall), None):
            original_total_cost_to_user = prr_with_slush_ge_shortfall.total_cost_to_user
            prr_with_slush_ge_shortfall.slush -= shortfall
            prr_with_slush_ge_shortfall.updated_at = pending_rewards_updated_at
            total_costs.append(
                TotalCostToUserDataSchema(
                    new_total_cost_to_user=prr_with_slush_ge_shortfall.total_cost_to_user,
                    original_total_cost_to_user=original_total_cost_to_user,
                    pending_reward_uuid=prr_with_slush_ge_shortfall.pending_reward_uuid,
                    pending_reward_updated_at=prr_with_slush_ge_shortfall.updated_at,
                )
            )
            shortfall = 0
            return current_balance, deleted_count_by_uuid, shortfall, total_costs

        # try to use collective slush of all available prrs to absorb shortfall
        for pending_reward in [prr for prr in pending_rewards if prr.slush > 0]:
            pending_reward.updated_at = pending_rewards_updated_at

            if pending_reward.slush >= shortfall:
                original_total_cost_to_user = pending_reward.total_cost_to_user
                pending_reward.total_cost_to_user -= shortfall
                total_costs.append(
                    TotalCostToUserDataSchema(
                        new_total_cost_to_user=pending_reward.total_cost_to_user,
                        original_total_cost_to_user=original_total_cost_to_user,
                        pending_reward_uuid=pending_reward.pending_reward_uuid,
                        pending_reward_updated_at=pending_reward.updated_at,
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
                    pending_reward_uuid=pending_reward.pending_reward_uuid,
                    pending_reward_updated_at=pending_reward.updated_at,
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

    async def allocate_pending_reward(
        self, transaction: Transaction, campaign: Campaign, count: int, total_cost_to_user: int
    ) -> None:
        pending_reward = await crud.create_pending_reward(
            self.db_session,
            account_holder_id=transaction.account_holder_id,
            campaign_id=campaign.id,
            allocation_window=campaign.reward_rule.allocation_window,
            value=campaign.reward_rule.reward_goal,
            count=count,
            total_cost_to_user=total_cost_to_user,
        )
        await self.store_activity(
            activity_type=RewardsActivityType.REWARD_STATUS,
            payload_formatter_fn=RewardsActivityType.get_reward_status_activity_data,
            formatter_kwargs={
                "account_holder_uuid": transaction.account_holder.account_holder_uuid,
                "retailer_slug": self.retailer.slug,
                "campaigns": [campaign.slug],
                "summary": f"{self.retailer.name} Pending reward issued for {campaign.name}",
                "new_status": "pending",
                "activity_datetime": pending_reward.created_date,
                "activity_identifier": str(pending_reward.pending_reward_uuid),
                "count": count,
            },
        )

    async def _process_rewards(
        self,
        *,
        transaction: Transaction,
        campaign: Campaign,
        rewards_achieved_n: int,
        trc_reached: bool,
        adjustment: int,
    ) -> tuple[int, list["RetryTask"]]:
        log_suffix = f"(tx_id: {transaction.transaction_id})"
        if trc_reached:
            tot_cost_to_user = adjustment
            logger.info("Transaction reward cap '%s' reached %s", campaign.reward_rule.reward_cap, log_suffix)
            logger.info(
                "Transaction reward cap reached, decreasing balance by original adjustment amount (%s) %s",
                tot_cost_to_user,
                log_suffix,
            )
        else:
            tot_cost_to_user = int(rewards_achieved_n * campaign.reward_rule.reward_goal)
            logger.info(
                "Reward goal (%d) met %d time%s %s",
                campaign.reward_rule.reward_goal,
                rewards_achieved_n,
                "s" if rewards_achieved_n > 1 else "",
                log_suffix,
            )
            logger.info("Decreasing balance by total rewards value (%s) %s", tot_cost_to_user, log_suffix)

        if campaign.reward_rule.allocation_window:
            await self.allocate_pending_reward(
                transaction=transaction,
                campaign=campaign,
                count=rewards_achieved_n,
                total_cost_to_user=tot_cost_to_user,
            )
            reward_issuance_tasks = []

        else:
            reward_issuance_tasks = await self._create_reward_issuance_tasks(
                transaction=transaction, campaign=campaign, num_rewards=rewards_achieved_n
            )

        return tot_cost_to_user, reward_issuance_tasks

    async def _create_reward_issuance_tasks(
        self, *, transaction: Transaction, campaign: Campaign, num_rewards: int
    ) -> list["RetryTask"]:
        return await async_create_many_tasks(
            self.db_session,
            task_type_name=reward_settings.REWARD_ISSUANCE_TASK_NAME,
            params_list=[
                {
                    "account_holder_id": transaction.account_holder_id,
                    "campaign_id": campaign.id,
                    "reward_config_id": campaign.reward_rule.reward_config_id,
                    "reason": IssuedRewardReasons.GOAL_MET.name,
                }
                for _ in range(num_rewards)
            ],
        )

    async def _process_balance_adjustments(
        self, campaigns: list[Campaign], transaction: Transaction, account_holder: "AccountHolder"
    ) -> tuple[dict[str, AdjustmentAmount], list["RetryTask"] | None]:
        locked_balances = await crud.get_balances_for_update(
            self.db_session, account_holder_id=account_holder.id, campaigns=campaigns
        )
        balances_by_campaign_id = {balance.campaign_id: balance for balance in locked_balances}

        adjustments: dict[str, AdjustmentAmount] = {}
        reward_issuance_tasks: list["RetryTask"] | None = None
        for campaign in campaigns:
            campaign_balance = balances_by_campaign_id[campaign.id]
            adjustment = await self._adjust_balance(
                campaign=campaign,
                campaign_balance=campaign_balance,
                transaction=transaction,
                account_holder_uuid=account_holder.account_holder_uuid,
            )

            adjustments[campaign.slug] = adjustment
            await crud.record_earn(
                self.db_session,
                campaign.loyalty_type,
                transaction.id,
                adjustment.amount if adjustment.accepted else None,
            )
            if adjustment.accepted and adjustment.amount > 0:
                rewards_achieved_n, trc_reached = self._rewards_achieved(
                    campaign, campaign_balance.balance, adjustment.amount
                )
                if rewards_achieved_n:
                    balance_reduction, reward_issuance_tasks = await self._process_rewards(
                        transaction=transaction,
                        campaign=campaign,
                        rewards_achieved_n=rewards_achieved_n,
                        trc_reached=trc_reached,
                        adjustment=adjustment.amount,
                    )
                    campaign_balance.balance -= balance_reduction

        return adjustments, reward_issuance_tasks

    async def update_payload_and_store_activity(self, activity_data: dict, updated_payload_data: dict) -> None:
        activity_data["formatter_kwargs"].update(updated_payload_data)
        await self.store_activity(**activity_data)

    async def get_active_campaigns(self, transaction_datetime: datetime) -> list[Campaign]:
        return [
            campaign
            for campaign in self.retailer.campaigns
            if campaign.status == CampaignStatuses.ACTIVE
            and campaign.start_date is not None
            and campaign.start_date <= transaction_datetime
            and (campaign.end_date is None or campaign.end_date > transaction_datetime)
        ]

    async def get_transaction_store_name(self, transaction_mid: str) -> str:
        return await crud.get_store_name_by_mid(self.db_session, mid=transaction_mid) or "N/A"

    async def _handle_incoming_transaction(  # noqa: PLR0911
        self, request_payload: CreateTransactionSchema, tx_import_activity_data: dict
    ) -> tuple[ServiceResult[str, ServiceError], list["RetryTask"] | None]:

        # database DateTimes are naive
        tx_datetime_naive = request_payload.transaction_datetime.replace(tzinfo=None)

        account_holder = await accounts_crud.get_account_holder(
            self.db_session, retailer_id=self.retailer.id, account_holder_uuid=request_payload.account_holder_uuid
        )
        if self.retailer.status == RetailerStatuses.INACTIVE:
            return ServiceResult(error=ServiceError(error_code=ErrorCode.INACTIVE_RETAILER)), None
        if not account_holder:
            return ServiceResult(error=ServiceError(error_code=ErrorCode.USER_NOT_FOUND)), None
        if account_holder.status != AccountHolderStatuses.ACTIVE:
            return ServiceResult(error=ServiceError(error_code=ErrorCode.USER_NOT_ACTIVE)), None

        if account_holder.created_at > tx_datetime_naive:
            return ServiceResult(error=ServiceError(error_code=ErrorCode.INVALID_TX_DATE)), None

        if not (active_campaigns := await self.get_active_campaigns(tx_datetime_naive)):
            return ServiceResult(error=ServiceError(error_code=ErrorCode.NO_ACTIVE_CAMPAIGNS)), None

        tx_import_activity_data["campaign_slugs"] = [cmp.slug for cmp in active_campaigns]

        transaction = await crud.create_transaction(
            self.db_session,
            account_holder_id=account_holder.id,
            retailer_id=self.retailer.id,
            transaction_data=request_payload,
            tx_datetime_naive=tx_datetime_naive,
        )
        if not transaction.processed:
            await self.commit_db_changes()
            return ServiceResult(error=ServiceError(error_code=ErrorCode.DUPLICATE_TRANSACTION)), None

        adjustment_amounts, reward_issuance_tasks = await self._process_balance_adjustments(
            active_campaigns, transaction, account_holder
        )
        await self.store_activity(
            activity_type=TransactionActivityType.TX_HISTORY,
            payload_formatter_fn=TransactionActivityType.get_processed_tx_activity_data,
            formatter_kwargs={
                "account_holder_uuid": account_holder.account_holder_uuid,
                "processed_tx": transaction,
                "retailer": self.retailer,
                "adjustment_amounts": adjustment_amounts,
                "store_name": await self.get_transaction_store_name(transaction.mid),
            },
            prepend=True,
        )

        is_refund = transaction.amount < 0
        accepted_adjustments = any(adjustment.accepted for adjustment in adjustment_amounts.values())

        tx_import_activity_data["invalid_refund"] = is_refund and not accepted_adjustments

        await self.commit_db_changes()
        return ServiceResult(self._get_transaction_response(accepted_adjustments, is_refund)), reward_issuance_tasks

    async def handle_incoming_transaction(
        self, request_payload: CreateTransactionSchema
    ) -> ServiceResult[str, ServiceError]:
        """Main handler for incoming transactions"""

        try:
            tx_import_activity_data = {
                "retailer": self.retailer,
                "campaign_slugs": [],
                "invalid_refund": False,
            }
            service_result, reward_issuance_tasks = await self._handle_incoming_transaction(
                request_payload, tx_import_activity_data
            )
            if service_result.error:
                tx_import_activity_data["error"] = service_result.error.error_code.name

        except Exception as exc:
            await self.clear_stored_activities()
            tx_import_activity_data["error"] = exc.__class__.__name__
            raise
        else:
            if reward_issuance_tasks:
                await self.trigger_asyncio_task(
                    enqueue_many_tasks(retry_tasks_ids=[t.retry_task_id for t in reward_issuance_tasks])
                )
        finally:
            tx_import_activity_data["request_payload"] = request_payload.dict()
            await self.store_activity(
                activity_type=TransactionActivityType.TX_IMPORT,
                payload_formatter_fn=TransactionActivityType.get_tx_import_activity_data,
                formatter_kwargs=tx_import_activity_data,
                prepend=True,
            )
            await self.format_and_send_stored_activities()

        return service_result
