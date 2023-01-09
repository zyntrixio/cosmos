import logging

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from sqlalchemy import literal
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.future import select
from sqlalchemy.orm import joinedload, noload, selectinload

from cosmos.accounts.enums import AccountHolderStatuses
from cosmos.campaigns.enums import CampaignStatuses
from cosmos.db.base_class import async_run_query
from cosmos.db.models import AccountHolder, Campaign, CampaignBalance, Retailer, Transaction

if TYPE_CHECKING:  # pragma: no cover
    from typing import NamedTuple

    from sqlalchemy.engine import Row
    from sqlalchemy.ext.asyncio import AsyncSession
    from sqlalchemy.ext.asyncio.session import AsyncSessionTransaction

    class ChangeCampaignStatusRes(NamedTuple):
        updated_at: datetime
        status: CampaignStatuses


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

    async def _query(savepoint: "AsyncSessionTransaction") -> None:
        select_stmt = select(AccountHolder.id, literal(campaign.id), literal(0), balance_reset_date).where(
            AccountHolder.retailer_id == retailer.id,
            AccountHolder.status == AccountHolderStatuses.ACTIVE,
        )
        await db_session.execute(
            insert(CampaignBalance)
            .from_select(["account_holder_id", "campaign_id", "balance", "reset_date"], select_stmt)
            .on_conflict_do_nothing()
        )
        await savepoint.commit()

    await async_run_query(_query, db_session)
    logger.info("Inserted campaign balances")


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
