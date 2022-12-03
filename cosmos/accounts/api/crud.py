from typing import TYPE_CHECKING

from pydantic import UUID4
from sqlalchemy.exc import IntegrityError
from sqlalchemy.future import select
from sqlalchemy.orm import aliased, contains_eager, joinedload

from cosmos.accounts.enums import AccountHolderStatuses
from cosmos.core.api.http_error import HttpErrors
from cosmos.db.base_class import async_run_query
from cosmos.db.models import AccountHolder, AccountHolderMarketingPreference, AccountHolderProfile, Transaction

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


class CrudError(Exception):
    pass


class AccountExists(CrudError):
    pass


class AccountHolderInactive(CrudError):
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
        raise AccountExists  # pylint: disable=raise-missing-from

    profile = AccountHolderProfile(account_holder_id=account_holder.id, **profile_data)
    db_session.add(profile)
    marketing_preferences = [
        AccountHolderMarketingPreference(account_holder_id=account_holder.id, **mp) for mp in marketing_preferences_data
    ]
    db_session.add_all(marketing_preferences)
    return account_holder


async def get_account_holder(
    db_session: "AsyncSession",
    *,
    email: str,
    account_number: str,
    retailer_id: int,
    fetch_rewards: bool = True,
    fetch_balances: bool = True,
    tx_qty: int | None = None,
    raise_404_if_inactive: bool = True,
    # **query_param: str | int | UUID4,
) -> AccountHolder:
    account_holder_alias = aliased(AccountHolder, name="account_holder_alias")
    stmt = select(account_holder_alias).where(
        account_holder_alias.email == email,
        account_holder_alias.account_number == account_number,
        account_holder_alias.retailer_id == retailer_id,
    )
    if fetch_rewards:
        stmt = stmt.options(
            joinedload(account_holder_alias.rewards),
            joinedload(account_holder_alias.pending_rewards),
        )
    if fetch_balances:
        stmt = stmt.options(  # join(account_holder_alias.current_balances).options(
            joinedload(
                account_holder_alias.current_balances,
            )
        )
    if tx_qty:
        subq = (
            select(Transaction)
            .join(AccountHolder)
            .where(
                AccountHolder.email == email,
                AccountHolder.account_number == account_number,
                AccountHolder.retailer_id == retailer_id,
                Transaction.processed.is_(True),
            )
            .order_by(Transaction.created_at.desc(), Transaction.id)
            .limit(tx_qty)
            .subquery(name="lastest_transactions")
        )
        stmt = stmt.outerjoin(subq).options(
            contains_eager(account_holder_alias.transactions, alias=subq),
        )

    async def _query() -> AccountHolder:
        return (await db_session.execute(stmt)).scalars().first()

    account_holder = await async_run_query(_query, db_session, rollback_on_exc=False)
    if not account_holder or (raise_404_if_inactive and account_holder.status == AccountHolderStatuses.INACTIVE):
        raise HttpErrors.NO_ACCOUNT_FOUND.value

    return account_holder
