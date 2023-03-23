from collections.abc import Callable, Iterable
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from unittest import mock
from zoneinfo import ZoneInfo

from deepdiff import DeepDiff
from pytest_mock import MockerFixture

from cosmos.accounts.activity.enums import ActivityType as AccountsActivityType
from cosmos.core.scheduled_tasks.balances import reset_balances
from cosmos.core.scheduled_tasks.scheduler import cron_scheduler
from cosmos.db.models import AccountHolder, AccountHolderEmail, Campaign, CampaignBalance, EmailTemplate

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


def test_reset_balances_ok_retailer_has_lifespan(
    mocker: MockerFixture,
    db_session: "Session",
    account_holder: "AccountHolder",
    create_campaign: Callable[..., "Campaign"],
    balance_reset_email_template: "EmailTemplate",
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

    account_holder_emails = [
        AccountHolderEmail(
            account_holder_id=account_holder.id,
            email_type_id=balance_reset_email_template.email_type_id,
            campaign_id=campaign_id,
            allow_re_send=False,
        )
        for campaign_id in (1, 2, 3)
    ]

    db_session.add_all(account_holder_emails)
    db_session.commit()

    sent_activities: list[dict] = []

    def mock_send_activities(activities: Iterable, *, routing_key: str) -> None:
        nonlocal sent_activities
        for activity in activities:
            del activity["id"]
            sent_activities.append(activity)

    mocker.patch("cosmos.core.scheduled_tasks.balances.sync_send_activity", mock_send_activities)

    reset_balances()
    db_session.expire_all()

    expected_activity_values = []
    for balance in balances:

        match balance.id:
            case 1 | 3:
                assert balance.balance == 0
                assert balance.reset_date == today + timedelta(account_holder.retailer.balance_lifespan)
                expected_activity_values.append(
                    {
                        "type": "BALANCE_CHANGE",
                        "datetime": now,
                        "underlying_datetime": balance.updated_at.replace(tzinfo=UTC),
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

    for account_holder_email in account_holder_emails:
        if account_holder_email.campaign_id in (1, 3):
            assert account_holder_email.allow_re_send
        else:
            assert not account_holder_email.allow_re_send


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
