import logging

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from sqlalchemy import func, literal
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.future import select
from sqlalchemy.orm import joinedload, noload, selectinload

from cosmos.accounts.enums import AccountHolderStatuses
from cosmos.campaigns.enums import CampaignStatuses, LoyaltyTypes
from cosmos.db.base_class import async_run_query
from cosmos.db.models import AccountHolder, Campaign, CampaignBalance, Retailer, Transaction

if TYPE_CHECKING:  # pragma: no cover
    from typing import NamedTuple
    from uuid import UUID

    from sqlalchemy.engine import Row
    from sqlalchemy.ext.asyncio import AsyncSession
    from sqlalchemy.ext.asyncio.session import AsyncSessionTransaction

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
    async def _query() -> Campaign:

        match load_rules, lock_row:  # noqa: E999
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
    db_session: "AsyncSession", retailer: Retailer, transaction: Transaction = None, join_rules: bool = False
) -> list[Campaign]:

    opt = [joinedload(Campaign.earn_rule), joinedload(Campaign.reward_rule)] if join_rules else []

    async def _query() -> list:
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
            if campaign.start_date <= transaction.datetime
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
    now = datetime.now(tz=timezone.utc).replace(tzinfo=None)
    update_values: dict[str, CampaignStatuses | datetime] = {"status": requested_status}

    match requested_status:
        case CampaignStatuses.ACTIVE:
            update_values["start_date"] = now
        case CampaignStatuses.CANCELLED | CampaignStatuses.ENDED:
            update_values["end_date"] = now

    async def _query(campaign_id: int, savepoint: "AsyncSessionTransaction") -> "Row":
        res = (
            await db_session.execute(
                Campaign.__table__.update()
                .values(**update_values)
                .where(Campaign.id == campaign_id)
                .returning(Campaign.updated_at, Campaign.status)
            )
        ).first()
        await savepoint.commit()
        return res

    return await async_run_query(_query, db_session, campaign_id=campaign.id)


async def create_campaign_balances(db_session: "AsyncSession", retailer: Retailer, campaign: Campaign) -> None:
    balance_reset_date = (
        (datetime.now(tz=timezone.utc) + timedelta(days=retailer.balance_lifespan)).date()
        if retailer.balance_lifespan
        else None
    )

    async def _query(savepoint: "AsyncSessionTransaction") -> int:
        select_stmt = select(AccountHolder.id, literal(campaign.id), literal(0), balance_reset_date).where(
            AccountHolder.retailer_id == retailer.id,
            AccountHolder.status == AccountHolderStatuses.ACTIVE,
        )
        res = await db_session.execute(
            insert(CampaignBalance)
            .from_select(["account_holder_id", "campaign_id", "balance", "reset_date"], select_stmt)
            .on_conflict_do_nothing()
        )
        await savepoint.commit()
        return res.rowcount

    inserted_rows = await async_run_query(_query, db_session)
    logger.info("Inserted %d campaign balances", inserted_rows)


async def delete_campaign_balances(db_session: "AsyncSession", retailer: Retailer, campaign: Campaign) -> None:
    async def _query(savepoint: "AsyncSessionTransaction") -> int:
        del_balance = await db_session.execute(
            CampaignBalance.__table__.delete().where(
                CampaignBalance.campaign_id == campaign.id,
                CampaignBalance.account_holder_id == AccountHolder.id,
                AccountHolder.retailer_id == retailer.id,
            )
        )

        await savepoint.commit()
        return del_balance.rowcount

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
            computed_balance = func.ceil(CampaignBalance.balance * rate_multiplier)
        case LoyaltyTypes.STAMPS:
            computed_balance = func.ceil((CampaignBalance.balance * rate_multiplier) / 100) * 100
        case _:
            raise ValueError(f"Unexpected loyalty type '{to_campaign.loyalty_type}' received.")

    async def _query(savepoint: "AsyncSessionTransaction") -> list["Row"]:
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
            CampaignBalance.__table__.update()
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

    return await async_run_query(_query, db_session)
