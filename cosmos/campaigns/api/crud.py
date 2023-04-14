import logging

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, NamedTuple, cast
from uuid import UUID

from sqlalchemy import Date, Table, delete, func, literal, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import joinedload, noload, selectinload

from cosmos.accounts.enums import AccountHolderStatuses
from cosmos.campaigns.enums import CampaignStatuses, LoyaltyTypes
from cosmos.db.base_class import async_run_query
from cosmos.db.models import AccountHolder, Campaign, CampaignBalance, Retailer, Transaction

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable

    from sqlalchemy.engine import Row
    from sqlalchemy.ext.asyncio import AsyncSession
    from sqlalchemy.ext.asyncio.session import AsyncSessionTransaction
    from sqlalchemy.sql.expression import Select


class ChangeCampaignStatusRes(NamedTuple):
    updated_at: datetime
    status: CampaignStatuses


class BalanceTransferRes(NamedTuple):
    account_holder_uuid: UUID
    balance: int


logger = logging.getLogger(__name__)


async def get_campaign_by_slug(
    db_session: "AsyncSession", campaign_slug: str, retailer: Retailer, load_rules: bool = False, lock_row: bool = True
) -> Campaign | None:
    async def _query() -> Campaign | None:
        option: "Callable"
        match load_rules, lock_row:
            case True, False:
                option = joinedload
            case True, True:
                option = selectinload
            case _:
                option = noload

        stmt = (
            select(Campaign)
            .options(option(Campaign.earn_rule), option(Campaign.reward_rule))
            .where(
                Campaign.slug == campaign_slug,
                Campaign.retailer_id == retailer.id,
            )
        )
        if lock_row:
            stmt = stmt.with_for_update()

        return await db_session.scalar(stmt)

    return await async_run_query(_query, db_session, rollback_on_exc=False)


async def get_active_campaigns(
    db_session: "AsyncSession", retailer: Retailer, transaction: Transaction | None = None, join_rules: bool = False
) -> Sequence[Campaign]:
    opt = [joinedload(Campaign.earn_rule), joinedload(Campaign.reward_rule)] if join_rules else []

    async def _query() -> Sequence[Campaign]:
        return (
            (
                await db_session.execute(
                    select(Campaign)
                    .options(*opt)
                    .where(Campaign.retailer_id == retailer.id, Campaign.status == CampaignStatuses.ACTIVE)
                )
            )
            .unique()
            .scalars()
            .all()
        )

    campaigns = await async_run_query(_query, db_session, rollback_on_exc=False)

    return (
        [
            campaign
            for campaign in campaigns
            if campaign.start_date
            and campaign.start_date <= transaction.datetime
            and (campaign.end_date is None or campaign.end_date > transaction.datetime)
        ]
        if transaction is not None
        else campaigns
    )


async def campaign_status_change(
    db_session: "AsyncSession",
    campaign: Campaign,
    requested_status: CampaignStatuses,
) -> "ChangeCampaignStatusRes":
    now = datetime.now(tz=UTC).replace(tzinfo=None)
    update_values: dict[str, CampaignStatuses | datetime] = {"status": requested_status}

    match requested_status:
        case CampaignStatuses.ACTIVE:
            update_values["start_date"] = now
        case CampaignStatuses.CANCELLED | CampaignStatuses.ENDED:
            update_values["end_date"] = now

    async def _query(campaign_id: int, savepoint: "AsyncSessionTransaction") -> tuple[datetime, CampaignStatuses]:
        res = (
            await db_session.execute(
                update(Campaign)
                .values(**update_values)
                .where(Campaign.id == campaign_id)
                .returning(Campaign.updated_at, Campaign.status)
            )
        ).one()
        await savepoint.commit()
        return res.tuple()

    res = await async_run_query(_query, db_session, campaign_id=campaign.id)
    return ChangeCampaignStatusRes(updated_at=res[0], status=res[1])


async def create_campaign_balances(db_session: "AsyncSession", retailer: Retailer, campaign: Campaign) -> None:
    balance_reset_date = (
        literal((datetime.now(tz=UTC) + timedelta(days=retailer.balance_lifespan)).date(), Date)
        if retailer.balance_lifespan
        else literal(None)
    )

    async def _query(savepoint: "AsyncSessionTransaction") -> int:
        select_stmt: "Select" = (
            select(AccountHolder.id, literal(campaign.id), literal(0), balance_reset_date)
            .select_from(AccountHolder)
            .where(
                AccountHolder.retailer_id == retailer.id,
                AccountHolder.status == AccountHolderStatuses.ACTIVE,
            )
        )
        res = await db_session.execute(
            insert(CampaignBalance)
            .from_select(["account_holder_id", "campaign_id", "balance", "reset_date"], select_stmt)
            .on_conflict_do_nothing()
            .returning(CampaignBalance.account_holder_id)
        )
        await savepoint.commit()
        return len(res.all())

    inserted_rows = await async_run_query(_query, db_session)
    logger.info("Inserted %d campaign balances", inserted_rows)


async def delete_campaign_balances(db_session: "AsyncSession", retailer: Retailer, campaign: Campaign) -> None:
    async def _query(savepoint: "AsyncSessionTransaction") -> int:
        del_balance = await db_session.execute(
            delete(CampaignBalance)
            .where(
                CampaignBalance.campaign_id == campaign.id,
                CampaignBalance.account_holder_id == AccountHolder.id,
                AccountHolder.retailer_id == retailer.id,
            )
            .returning(CampaignBalance.id)
        )

        await savepoint.commit()
        return len(del_balance.all())

    del_balance = await async_run_query(_query, db_session)
    logger.info("Deleted %d campaign balances", del_balance)


async def lock_balances_for_campaign(db_session: "AsyncSession", *, campaign: Campaign) -> None:
    """Explicity lock all balances for the provided campaign"""

    async def _query() -> None:
        await db_session.execute(
            select(CampaignBalance).where(CampaignBalance.campaign_id == campaign.id).with_for_update()
        )

    await async_run_query(_query, db_session, rollback_on_exc=False)


async def transfer_balance(
    db_session: "AsyncSession", *, from_campaign: Campaign, to_campaign: Campaign, threshold: int, rate_percent: int
) -> list["BalanceTransferRes"]:
    min_balance = int((from_campaign.reward_rule.reward_goal / 100) * threshold)
    rate_multiplier = rate_percent / 100

    match to_campaign.loyalty_type:
        case LoyaltyTypes.ACCUMULATOR:
            computed_balance = func.ceil(CampaignBalance.balance * rate_multiplier) * 1
        case LoyaltyTypes.STAMPS:
            computed_balance = func.ceil((CampaignBalance.balance * rate_multiplier) / 100) * 100
        case _:
            raise ValueError(f"Unexpected loyalty type '{to_campaign.loyalty_type}' received.")

    async def _query(savepoint: "AsyncSessionTransaction") -> Sequence["Row"]:
        update_values_cte = (
            select(
                CampaignBalance.account_holder_id,
                computed_balance.label("balance"),
                CampaignBalance.reset_date,
            )
            .where(
                CampaignBalance.campaign_id == from_campaign.id,
                CampaignBalance.balance >= min_balance,
                CampaignBalance.balance > 0,
            )
            .cte("update_values_cte")
        )
        update_stmt = (
            cast(Table, CampaignBalance.__table__)
            .update()
            .values(
                balance=update_values_cte.c.balance,
                reset_date=update_values_cte.c.reset_date,
            )
            .where(
                CampaignBalance.campaign_id == to_campaign.id,
                CampaignBalance.account_holder_id == update_values_cte.c.account_holder_id,
                # this is needed to return the account holder uuid
                CampaignBalance.account_holder_id == AccountHolder.id,
            )
            .returning(
                AccountHolder.account_holder_uuid,
                CampaignBalance.balance,
            )
        )
        res = await db_session.execute(update_stmt)
        await savepoint.commit()
        return res.all()

    res = await async_run_query(_query, db_session)
    return [BalanceTransferRes(account_holder_uuid=r[0], balance=r[1]) for r in res]
