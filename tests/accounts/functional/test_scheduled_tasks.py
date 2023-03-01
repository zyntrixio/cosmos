from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Callable, Iterable
from unittest import mock
from zoneinfo import ZoneInfo

from deepdiff import DeepDiff
from pytest_mock import MockerFixture
from retry_tasks_lib.db.models import RetryTask, TaskType
from sqlalchemy.future import select

from cosmos.accounts.activity.enums import ActivityType as AccountsActivityType
from cosmos.accounts.config import account_settings
from cosmos.campaigns.enums import LoyaltyTypes
from cosmos.core.config import core_settings, redis
from cosmos.core.scheduled_tasks.balances import reset_balances, send_balance_reset_nudges
from cosmos.core.scheduled_tasks.scheduler import cron_scheduler
from cosmos.db.models import AccountHolder, Campaign, CampaignBalance
from tests.conftest import SetupType

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


def test_reset_balances_ok_ratailer_has_lifespan(
    mocker: MockerFixture,
    db_session: "Session",
    account_holder: "AccountHolder",
    create_campaign: Callable[..., "Campaign"],
) -> None:
    now = datetime.now(tz=ZoneInfo(cron_scheduler.tz))

    mock_logger = mocker.patch("cosmos.core.scheduled_tasks.balances.logger")
    mock_datetime = mocker.patch("cosmos.core.scheduled_tasks.balances.datetime")
    mock_datetime.now.return_value = now

    today = now.date()
    delta = timedelta(days=1)
    account_holder.retailer.balance_lifespan = 10
    account_holder.retailer.balance_reset_advanced_warning_days = 3  # must be set if balance_lifespan set

    campaigns = [
        create_campaign(id=campaign_id, slug=campaign_slug)
        for campaign_id, campaign_slug in (
            (1, "test-campaign-today"),
            (2, "test-campaign-tomorrow"),
            (3, "test-campaign-yesterday"),
        )
    ]

    db_session.add_all(campaigns)

    balances = [
        CampaignBalance(
            balance=100,
            campaign_id=campaign_id,
            account_holder_id=account_holder.id,
            reset_date=reset_date,
        )
        for campaign_id, reset_date in (
            (1, today),
            (2, today + delta),
            (3, today - delta),
        )
    ]

    db_session.add_all(balances)
    db_session.commit()

    sent_activities: list[dict] = []

    def mock_send_activities(activities: Iterable, *, routing_key: str) -> None:
        nonlocal sent_activities
        for activity in activities:
            del activity["id"]
            sent_activities.append(activity)

    mocker.patch("cosmos.core.scheduled_tasks.balances.sync_send_activity", mock_send_activities)

    reset_balances()

    expected_activity_values = []
    for balance in balances:
        db_session.refresh(balance)

        match balance.id:  # noqa [E999]
            case 1 | 3:
                assert balance.balance == 0
                assert balance.reset_date == today + timedelta(account_holder.retailer.balance_lifespan)
                expected_activity_values.append(
                    {
                        "type": "BALANCE_CHANGE",
                        "datetime": now,
                        "underlying_datetime": balance.updated_at.replace(tzinfo=timezone.utc),
                        "summary": f"re-test {balance.campaign.slug} Balance 100",
                        "reasons": ["Balance Reset every 10 days"],
                        "activity_identifier": "N/A",
                        "user_id": str(account_holder.account_holder_uuid),
                        "associated_value": "0",
                        "retailer": "re-test",
                        "campaigns": [balance.campaign.slug],
                        "data": {
                            "new_balance": 0,
                            "original_balance": 100,
                            "reset_date": balance.reset_date.isoformat(),
                        },
                    },
                )

            case 2:
                assert balance.balance == 100
                assert balance.reset_date == today + delta

    assert mock_logger.info.call_count == 2
    assert mock_logger.info.call_args_list[0] == mock.call("Running scheduled balance reset.")
    assert mock_logger.info.call_args_list[1] == mock.call(
        "Operation completed successfully, %d balances have been set to 0", 2
    )
    assert not DeepDiff(sent_activities, expected_activity_values)


def test_reset_balances_ok_ratailer_does_not_have_lifespan(
    mocker: MockerFixture,
    db_session: "Session",
    create_campaign: Callable[..., "Campaign"],
    account_holder: "AccountHolder",
) -> None:
    mock_logger = mocker.patch("cosmos.core.scheduled_tasks.balances.logger")
    mock_send_activity = mocker.patch("cosmos.core.scheduled_tasks.balances.sync_send_activity")
    today = datetime.now(tz=ZoneInfo(cron_scheduler.tz)).date()
    delta = timedelta(days=1)
    assert not account_holder.retailer.balance_lifespan

    campaigns = [
        create_campaign(id=campaign_id, slug=campaign_slug)
        for campaign_id, campaign_slug in (
            (1, "test-campaign-today"),
            (2, "test-campaign-tomorrow"),
            (3, "test-campaign-yesterday"),
        )
    ]

    db_session.add_all(campaigns)

    balances = [
        CampaignBalance(
            balance=100,
            campaign_id=campaign_id,
            account_holder_id=account_holder.id,
            reset_date=reset_date,
        )
        for campaign_id, reset_date in (
            (1, today),
            (2, today + delta),
            (3, today - delta),
        )
    ]

    db_session.add_all(balances)
    db_session.commit()

    reset_balances()

    for balance in balances:
        db_session.refresh(balance)

        match balance.campaign.slug:
            case "test-campaign-today" | "test-campaign-yesterday":
                assert balance.balance == 0
                assert balance.reset_date is None
            case "test-campaign-tomorrow":
                assert balance.balance == 100
                assert balance.reset_date == today + delta

    assert mock_logger.info.call_count == 2
    assert mock_logger.info.call_args_list[0] == mock.call("Running scheduled balance reset.")
    assert mock_logger.info.call_args_list[1] == mock.call(
        "Operation completed successfully, %d balances have been set to 0", 2
    )
    mock_send_activity.assert_called_once_with(mocker.ANY, routing_key=AccountsActivityType.BALANCE_CHANGE.value)


def test_send_balance_reset_nudges_ok(
    mocker: MockerFixture, setup: SetupType, create_campaign: Callable[..., "Campaign"], send_email_task_type: TaskType
) -> None:
    redis.delete(f"{core_settings.REDIS_KEY_PREFIX}{cron_scheduler.name}:{send_balance_reset_nudges.__qualname__}")
    db_session, retailer, account_holder = setup
    now = datetime.now(tz=ZoneInfo(cron_scheduler.tz))
    mock_logger = mocker.patch("cosmos.core.scheduled_tasks.balances.logger")
    mock_datetime = mocker.patch("cosmos.core.scheduled_tasks.balances.datetime")
    mock_datetime.now.return_value = now
    mock_enqueue = mocker.patch("cosmos.core.scheduled_tasks.balances.enqueue_many_retry_tasks")
    today = datetime.now(tz=ZoneInfo(cron_scheduler.tz)).date()
    retailer.balance_lifespan = 10
    retailer.balance_reset_advanced_warning_days = 5
    trigger_campaign = create_campaign(id=2, slug="test-campaign-trigger")
    trigger_stamp_campaign = create_campaign(id=4, slug="test-stamp-campaign-trigger", loyalty_type=LoyaltyTypes.STAMPS)
    non_trigger_campaign = create_campaign(id=3, slug="test-campaign-non-trigger")

    trigger_balance = CampaignBalance(
        balance=100,
        campaign_id=trigger_campaign.id,
        account_holder_id=account_holder.id,
        reset_date=today + timedelta(retailer.balance_reset_advanced_warning_days),
    )
    db_session.add(trigger_balance)
    trigger_stamp_balance = CampaignBalance(
        balance=200,
        campaign_id=trigger_stamp_campaign.id,
        account_holder_id=account_holder.id,
        reset_date=today + timedelta(retailer.balance_reset_advanced_warning_days),
    )
    db_session.add(trigger_stamp_balance)
    db_session.add(
        CampaignBalance(
            balance=200,
            campaign_id=non_trigger_campaign.id,
            account_holder_id=account_holder.id,
            reset_date=today,
        )
    )

    db_session.commit()

    send_balance_reset_nudges()

    assert mock_logger.info.call_args_list[0] == mock.call("Enqueueing email nudges tasks for balance resets.")
    assert mock_logger.info.call_args_list[1] == mock.call(
        "%d %s %s tasks enqueued.", 2, "BALANCE_RESET", account_settings.SEND_EMAIL_TASK_NAME
    )

    send_email_task = (
        db_session.execute(
            select(RetryTask).join(TaskType).where(TaskType.name == account_settings.SEND_EMAIL_TASK_NAME).limit(2)
        )
        .scalars()
        .unique()
    )
    task_ids = []
    for task in send_email_task:
        task_ids.append(task.retry_task_id)
        if task.get_params()["extra_params"]["campaign_slug"] == trigger_campaign.slug:
            assert task.get_params() == {
                "account_holder_id": account_holder.id,
                "template_type": "BALANCE_RESET",
                "retailer_id": retailer.id,
                "extra_params": {
                    "current_balance": "1.00",
                    "balance_reset_date": trigger_balance.reset_date.strftime("%d/%m/%Y"),
                    "datetime": now.strftime("%H:%M %d/%m/%Y"),
                    "campaign_slug": trigger_campaign.slug,
                    "retailer_slug": retailer.slug,
                    "retailer_name": retailer.name,
                    "account_holder_uuid": str(account_holder.account_holder_uuid),
                },
            }
        else:
            assert task.get_params() == {
                "account_holder_id": account_holder.id,
                "template_type": "BALANCE_RESET",
                "retailer_id": retailer.id,
                "extra_params": {
                    "current_balance": "2 stamps",
                    "balance_reset_date": trigger_balance.reset_date.strftime("%d/%m/%Y"),
                    "datetime": now.strftime("%H:%M %d/%m/%Y"),
                    "campaign_slug": trigger_stamp_campaign.slug,
                    "retailer_slug": retailer.slug,
                    "retailer_name": retailer.name,
                    "account_holder_uuid": str(account_holder.account_holder_uuid),
                },
            }
    mock_enqueue.assert_called_with(db_session=mocker.ANY, retry_tasks_ids=task_ids, connection=mocker.ANY)
