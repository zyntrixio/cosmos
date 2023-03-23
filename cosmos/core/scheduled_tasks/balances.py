from datetime import UTC, datetime
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from sqlalchemy import Date, literal, tuple_
from sqlalchemy.future import select

from cosmos.accounts.activity.enums import ActivityType as AccountsActivityType
from cosmos.core.activity.tasks import sync_send_activity
from cosmos.core.scheduled_tasks.scheduler import acquire_lock, cron_scheduler
from cosmos.db.models import AccountHolder, AccountHolderEmail, Campaign, CampaignBalance, EmailType, Retailer
from cosmos.db.session import SyncSessionMaker
from cosmos.retailers.enums import EmailTypeSlugs

from . import logger

if TYPE_CHECKING:
    from sqlalchemy.engine import Row
    from sqlalchemy.orm import Session


def _retrieve_and_update_balances(db_session: "Session") -> list["Row"]:
    today = datetime.now(tz=ZoneInfo(cron_scheduler.tz)).date()
    balances_to_update = (
        select(
            CampaignBalance.id.label("balance_id"),
            (literal(today, Date) + Retailer.balance_lifespan).label("reset_date"),
            CampaignBalance.balance.label("old_balance"),
            Retailer.balance_lifespan,
            Retailer.slug.label("retailer_slug"),
            AccountHolder.account_holder_uuid,
            Campaign.slug.label("campaign_slug"),
        )
        .select_from(CampaignBalance)
        .join(AccountHolder)
        .join(Retailer)
        .join(Campaign, CampaignBalance.campaign_id == Campaign.id)
        .where(CampaignBalance.reset_date <= today, Retailer.balance_lifespan is not None)
    ).cte("balances_to_update")

    update_stmt = (
        CampaignBalance.__table__.update()
        .values(balance=0, reset_date=balances_to_update.c.reset_date)
        .where(
            CampaignBalance.id == balances_to_update.c.balance_id,
        )
        .returning(
            CampaignBalance.account_holder_id,
            CampaignBalance.campaign_id,
            CampaignBalance.reset_date,
            CampaignBalance.updated_at,
            CampaignBalance.id,
            balances_to_update.c.retailer_slug,
            balances_to_update.c.balance_lifespan,
            balances_to_update.c.old_balance,
            balances_to_update.c.account_holder_uuid,
            balances_to_update.c.campaign_slug,
        )
    )
    res = db_session.execute(update_stmt).all()
    db_session.flush()
    # re-enables BALANCE_RESET nudges for updated account holders.
    db_session.execute(
        AccountHolderEmail.__table__.update()
        .values(allow_re_send=True)
        .where(
            AccountHolderEmail.email_type_id == EmailType.id,
            EmailType.slug == EmailTypeSlugs.BALANCE_RESET.name,
            AccountHolderEmail.allow_re_send.is_(False),
            tuple_(AccountHolderEmail.account_holder_id, AccountHolderEmail.campaign_id).in_(
                [(row.account_holder_id, row.campaign_id) for row in res]
            ),
        )
    )

    db_session.commit()
    return res


@acquire_lock(runner=cron_scheduler)
def reset_balances() -> None:
    logger.info("Running scheduled balance reset.")
    with SyncSessionMaker() as db_session:
        updated_balances = _retrieve_and_update_balances(db_session)
        logger.info("Operation completed successfully, %d balances have been set to 0", len(updated_balances))
        activity_datetime = datetime.now(tz=UTC)
        sync_send_activity(
            (
                AccountsActivityType.get_balance_reset_activity_data(
                    reset_date=updated_balance.reset_date,
                    activity_datetime=activity_datetime,
                    underlying_datetime=updated_balance.updated_at,
                    retailer_slug=updated_balance.retailer_slug,
                    balance_lifespan=updated_balance.balance_lifespan,
                    campaign_slug=updated_balance.campaign_slug,
                    old_balance=updated_balance.old_balance,
                    account_holder_uuid=updated_balance.account_holder_uuid,
                )
                for updated_balance in updated_balances
            ),
            routing_key=AccountsActivityType.BALANCE_CHANGE.value,
        )
