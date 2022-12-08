from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, cast

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import noload

from cosmos.db.base_class import async_run_query
from cosmos.db.models import CampaignBalance, PendingReward, Transaction, TransactionCampaign
from cosmos.transactions.api.schemas import CreateTransactionSchema

if TYPE_CHECKING:
    from uuid import UUID

    from cosmos.db.models import Campaign, Retailer


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

    transaction_kwargs = dict(
        account_holder_id=account_holder_id,
        retailer_id=retailer_id,
        transaction_id=transaction_data.transaction_id,
        amount=transaction_data.amount,
        mid=transaction_data.mid,
        datetime=cast(datetime, transaction_data.datetime).replace(tzinfo=None),
        payment_transaction_id=transaction_data.payment_transaction_id,
    )

    async def _query() -> Transaction:
        try:
            nested_trans = await db_session.begin_nested()
            transaction = Transaction(processed=True, **transaction_kwargs)
            db_session.add(transaction)
            await nested_trans.commit()
            return transaction
        except IntegrityError:
            await nested_trans.rollback()
            transaction = Transaction(processed=None, **transaction_kwargs)
            db_session.add(transaction)
            await db_session.flush()
            return transaction

    return await async_run_query(_query, db_session)


async def associate_campaign_to_transaction(
    db_session: "AsyncSession", campaign_id: int, transaction_id: int, adjustment: int | None
) -> TransactionCampaign:
    async def _query() -> TransactionCampaign:
        transaction_campaign = TransactionCampaign(
            campaign_id=campaign_id, transaction_id=transaction_id, adjustment=adjustment
        )
        db_session.add(transaction_campaign)
        db_session.flush()
        return transaction_campaign

    return await async_run_query(_query, db_session)


async def create_pending_reward(
    db_session: "AsyncSession",
    *,
    account_holder_id: int,
    campaign_id: int,
    conversion_date: date,
    value: int,
    count: int,
    total_cost_to_user: int,
) -> PendingReward:
    pending_reward = PendingReward(
        account_holder_id=account_holder_id,
        campaign_id=campaign_id,
        conversion_date=conversion_date,
        created_date=datetime.now(tz=timezone.utc).replace(tzinfo=None),
        value=value,
        count=count,
        total_cost_to_user=total_cost_to_user,
    )

    async def _query() -> None:
        db_session.add(pending_reward)
        await db_session.flush()

    await async_run_query(_query, db_session, rollback_on_exc=False)
    return pending_reward
