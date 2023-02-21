from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import noload

from cosmos.campaigns.enums import LoyaltyTypes
from cosmos.db.base_class import async_run_query
from cosmos.db.models import CampaignBalance, PendingReward, RetailerStore, Transaction, TransactionEarn
from cosmos.transactions.api.schemas import CreateTransactionSchema

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio.session import AsyncSessionTransaction

    from cosmos.db.models import Campaign


async def get_balances_for_update(
    db_session: "AsyncSession", *, account_holder_id: int, campaigns: list["Campaign"]
) -> list[CampaignBalance]:
    async def _query() -> list[CampaignBalance]:
        return (
            (
                await (
                    db_session.execute(
                        select(CampaignBalance)
                        .where(
                            CampaignBalance.account_holder_id == account_holder_id,
                            CampaignBalance.campaign_id.in_([campaign.id for campaign in campaigns]),
                        )
                        .with_for_update()
                    )
                )
            )
            .scalars()
            .all()
        )

    return await async_run_query(_query, db_session, rollback_on_exc=False)


async def get_pending_rewards_for_update(
    db_session: "AsyncSession", *, account_holder_id: int, campaign_id: int
) -> list[PendingReward]:
    async def _query() -> list[PendingReward]:
        return (
            (
                await db_session.execute(
                    select(PendingReward)
                    .options(noload(PendingReward.account_holder))
                    .where(
                        PendingReward.account_holder_id == account_holder_id,
                        PendingReward.campaign_id == campaign_id,
                    )
                    .with_for_update()
                    .order_by(PendingReward.created_date.desc())
                )
            )
            .scalars()
            .all()
        )

    return await async_run_query(_query, db_session, rollback_on_exc=False)


async def delete_pending_reward(db_session: "AsyncSession", pending_reward: PendingReward) -> None:
    return await db_session.delete(pending_reward)


async def create_transaction(
    db_session: "AsyncSession",
    *,
    account_holder_id: int,
    retailer_id: int,
    transaction_data: CreateTransactionSchema,
) -> Transaction:

    transaction_kwargs = {
        "account_holder_id": account_holder_id,
        "retailer_id": retailer_id,
        "transaction_id": transaction_data.transaction_id,
        "amount": transaction_data.amount,
        "mid": transaction_data.mid,
        "datetime": transaction_data.transaction_datetime.replace(tzinfo=None),
        "payment_transaction_id": transaction_data.payment_transaction_id,
    }

    async def _query(savepoint: "AsyncSessionTransaction") -> Transaction:
        try:
            transaction = Transaction(processed=True, **transaction_kwargs)
            db_session.add(transaction)
            await savepoint.commit()

        except IntegrityError:
            await savepoint.rollback()
            savepoint = await db_session.begin_nested()
            transaction = Transaction(processed=None, **transaction_kwargs)
            db_session.add(transaction)
            await savepoint.commit()

        return transaction

    return await async_run_query(_query, db_session)


async def record_earn(
    db_session: "AsyncSession",
    loyalty_type: LoyaltyTypes,
    earn_rule_id: int,
    transaction_id: int,
    adjustment: int | None,
) -> TransactionEarn:
    async def _query(savepoint: "AsyncSessionTransaction") -> TransactionEarn:
        transaction_campaign = TransactionEarn(
            transaction_id=transaction_id, earn_rule_id=earn_rule_id, loyalty_type=loyalty_type, earn_amount=adjustment
        )
        db_session.add(transaction_campaign)
        await savepoint.commit()
        return transaction_campaign

    return await async_run_query(_query, db_session)


async def create_pending_reward(
    db_session: "AsyncSession",
    *,
    account_holder_id: int,
    campaign_id: int,
    allocation_window: int,
    value: int,
    count: int,
    total_cost_to_user: int,
) -> PendingReward:
    now = datetime.now(tz=timezone.utc)
    conversion_date = (now + timedelta(days=allocation_window)).date()

    async def _query(savepoint: "AsyncSessionTransaction") -> PendingReward:
        pending_reward = PendingReward(
            account_holder_id=account_holder_id,
            campaign_id=campaign_id,
            conversion_date=conversion_date,
            created_date=now.replace(tzinfo=None),
            value=value,
            count=count,
            total_cost_to_user=total_cost_to_user,
        )
        db_session.add(pending_reward)
        await savepoint.commit()
        return pending_reward

    return await async_run_query(_query, db_session)


async def get_store_name_by_mid(db_session: "AsyncSession", *, mid: str) -> str | None:
    async def _query() -> str | None:
        return (
            await db_session.execute(select(RetailerStore.store_name).where(RetailerStore.mid == mid))
        ).scalar_one_or_none()

    return await async_run_query(_query, db_session, rollback_on_exc=False)
