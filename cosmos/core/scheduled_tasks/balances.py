from datetime import datetime, timezone
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from retry_tasks_lib.utils.synchronous import enqueue_many_retry_tasks, sync_create_many_tasks
from sqlalchemy import Date, literal
from sqlalchemy.future import select

from cosmos.accounts.activity.enums import ActivityType as AccountsActivityType
from cosmos.accounts.config import account_settings
from cosmos.campaigns.enums import LoyaltyTypes
from cosmos.core.activity.tasks import sync_send_activity
from cosmos.core.config import redis_raw
from cosmos.core.scheduled_tasks.scheduler import acquire_lock, cron_scheduler
from cosmos.core.utils import pence_integer_to_currency_string, raw_stamp_value_to_string
from cosmos.db.models import AccountHolder, Campaign, CampaignBalance, Retailer
from cosmos.db.session import SyncSessionMaker
from cosmos.retailers.enums import EmailTemplateTypes

from . import logger

if TYPE_CHECKING:
    from retry_tasks_lib.db.models import RetryTask
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
        .subquery("balances_to_update")
    )
    update_stmt = (
        CampaignBalance.__table__.update()
        .values(balance=0, reset_date=balances_to_update.c.reset_date)
        .where(
            CampaignBalance.id == balances_to_update.c.balance_id,
        )
        .returning(
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
    db_session.commit()
    return res


def _create_advanced_warning_email_tasks(db_session: "Session") -> list["RetryTask"]:
    tz_info = ZoneInfo(cron_scheduler.tz)
    today = datetime.now(tz=tz_info).date()
    balance_reset_retailers_cte = (
        select(
            Retailer.id.label("retailer_id"),
            Retailer.balance_reset_advanced_warning_days.label("nudge_delta"),
        )
        .where(Retailer.balance_lifespan is not None)
        .cte("balance_reset_retailers")
    )
    ah_to_notify_stmt = db_session.execute(
        select(
            AccountHolder.id.label("account_holder_id"),
            AccountHolder.account_holder_uuid,
            AccountHolder.retailer_id,
            CampaignBalance.balance,
            CampaignBalance.reset_date,
            Retailer.slug.label("retailer_slug"),
            Retailer.name.label("retailer_name"),
            Campaign.slug.label("campaign_slug"),
            Campaign.loyalty_type,
        )
        .select_from(CampaignBalance)
        .join(AccountHolder)
        .join(Retailer)
        .join(Campaign, CampaignBalance.campaign_id == Campaign.id)
        .where(
            AccountHolder.retailer_id == balance_reset_retailers_cte.c.retailer_id,
            CampaignBalance.reset_date == literal(today, Date) + balance_reset_retailers_cte.c.nudge_delta,
            CampaignBalance.balance > 0,
        )
    ).all()
    lookup_time = datetime.now(tz=tz_info)

    email_tasks = sync_create_many_tasks(
        db_session,
        task_type_name=account_settings.SEND_EMAIL_TASK_NAME,
        params_list=[
            {
                "account_holder_id": data["account_holder_id"],
                "template_type": EmailTemplateTypes.BALANCE_RESET.name,
                "retailer_id": data["retailer_id"],
                "extra_params": {
                    "current_balance": pence_integer_to_currency_string(
                        data["balance"], currency="GBP", currency_sign=False
                    )
                    if data["loyalty_type"] == LoyaltyTypes.ACCUMULATOR
                    else raw_stamp_value_to_string(data["balance"]),
                    "balance_reset_date": data["reset_date"].strftime("%d/%m/%Y"),
                    "datetime": lookup_time.strftime("%H:%M %d/%m/%Y"),
                    "campaign_slug": data["campaign_slug"],
                    "retailer_slug": data["retailer_slug"],
                    "retailer_name": data["retailer_name"],
                    "account_holder_uuid": str(data["account_holder_uuid"]),
                },
            }
            for data in ah_to_notify_stmt
        ],
    )
    db_session.commit()
    return email_tasks


@acquire_lock(runner=cron_scheduler)
def reset_balances() -> None:
    logger.info("Running scheduled balance reset.")
    with SyncSessionMaker() as db_session:
        updated_balances = _retrieve_and_update_balances(db_session)
        logger.info("Operation completed successfully, %d balances have been set to 0", len(updated_balances))
        activity_datetime = datetime.now(tz=timezone.utc)
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


@acquire_lock(runner=cron_scheduler)
def send_balance_reset_nudges() -> None:
    logger.info("Enqueueing email nudges tasks for balance resets.")
    with SyncSessionMaker() as db_session:

        send_email_tasks = _create_advanced_warning_email_tasks(db_session)
        enqueue_many_retry_tasks(
            db_session=db_session,
            retry_tasks_ids=[task.retry_task_id for task in send_email_tasks],
            connection=redis_raw,
        )
    logger.info(
        "%d %s %s tasks enqueued.",
        len(send_email_tasks),
        EmailTemplateTypes.BALANCE_RESET.name,
        account_settings.SEND_EMAIL_TASK_NAME,
    )
