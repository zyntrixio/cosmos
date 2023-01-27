from typing import TYPE_CHECKING

from pydantic import UUID4, EmailStr
from sqlalchemy import and_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.future import select
from sqlalchemy.orm import aliased, contains_eager, joinedload, raiseload

from cosmos.accounts.enums import AccountHolderStatuses
from cosmos.db.base_class import async_run_query
from cosmos.db.models import (
    AccountHolder,
    AccountHolderProfile,
    Campaign,
    CampaignBalance,
    MarketingPreference,
    PendingReward,
    Retailer,
    RetailerStore,
    Reward,
    Transaction,
    TransactionEarn,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


class CrudError(Exception):
    pass


class AccountExistsError(CrudError):
    pass


class AccountHolderInactiveError(CrudError):
    pass


async def create_account_holder(
    db_session: "AsyncSession",
    *,
    email: str,
    retailer_id: int,
    profile_data: dict,
    marketing_preferences_data: list[dict],
) -> AccountHolder:
    account_holder = AccountHolder(email=email, retailer_id=retailer_id, status=AccountHolderStatuses.PENDING)
    nested = await db_session.begin_nested()
    try:
        db_session.add(account_holder)
        await nested.commit()
    except IntegrityError:
        await nested.rollback()
        raise AccountExistsError from None

    profile = AccountHolderProfile(account_holder_id=account_holder.id, **profile_data)
    db_session.add(profile)
    marketing_preferences = [
        MarketingPreference(account_holder_id=account_holder.id, **mp) for mp in marketing_preferences_data
    ]
    db_session.add_all(marketing_preferences)
    return account_holder


async def get_account_holder(
    db_session: "AsyncSession",
    *,
    retailer_id: int,
    fetch_rewards: bool = False,
    fetch_balances: bool = False,
    tx_qty: int | None = None,
    **account_holder_query_params: str | int | UUID4 | EmailStr,
) -> AccountHolder | None:
    """
    Get a single account holder based on query params. Ensure query params will
    return one single AccountHolder instance or none at all (since we use
    scalar_one_or_none here).

    Also optionally load Rewards, PendingRewards, Balances and Transactions
    using the relevant flags/params.
    """
    account_holder_alias = aliased(AccountHolder, name="account_holder_alias")
    stmt = (
        select(account_holder_alias)
        .filter_by(retailer_id=retailer_id, **account_holder_query_params)
        .options(joinedload(account_holder_alias.retailer).load_only(Retailer.status))
    )
    if fetch_rewards:
        stmt = stmt.options(
            joinedload(account_holder_alias.rewards)
            .load_only(
                Reward.code,
                Reward.issued_date,
                Reward.expiry_date,
                Reward.redeemed_date,
                Reward.cancelled_date,
                Reward.account_holder_id,
            )
            .joinedload(Reward.campaign)
            .load_only(Campaign.slug),
            joinedload(account_holder_alias.pending_rewards)
            .load_only(
                PendingReward.created_date,
                PendingReward.conversion_date,
                PendingReward.count,
            )
            .joinedload(PendingReward.campaign)
            .load_only(Campaign.slug),
        )
    if fetch_balances:
        stmt = stmt.options(
            joinedload(
                account_holder_alias.current_balances,
            )
            .load_only(CampaignBalance.balance)
            .joinedload(CampaignBalance.campaign)
            .load_only(Campaign.slug)
        )
    if tx_qty:
        cte = (
            select(Transaction)
            .join(Transaction.account_holder)
            .where(
                and_(*[(getattr(AccountHolder, k) == v) for k, v in account_holder_query_params.items()]),
                Transaction.processed.is_(True),
            )
            .order_by(Transaction.datetime.desc(), Transaction.id)
            .limit(tx_qty)
            .cte("latest_tx_subq")
        )

        stmt = stmt.outerjoin(cte, cte.c.account_holder_id == account_holder_alias.id).options(
            contains_eager(account_holder_alias.transactions, alias=cte).options(
                joinedload(Transaction.store).load_only(
                    RetailerStore.store_name,
                    RetailerStore.mid,
                ),
                joinedload(Transaction.transaction_earns).load_only(
                    TransactionEarn.earn_amount,
                    TransactionEarn.loyalty_type,
                    TransactionEarn.transaction_id,
                ),
            )
        )

    stmt = stmt.options(raiseload("*"))

    async def _query() -> AccountHolder:
        return (await db_session.execute(stmt)).unique().scalar_one_or_none()

    account_holder = await async_run_query(_query, db_session, rollback_on_exc=False)

    return account_holder
