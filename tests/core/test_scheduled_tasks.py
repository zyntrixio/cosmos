from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from deepdiff import DeepDiff
from retry_tasks_lib.db.models import RetryTask
from retry_tasks_lib.enums import RetryTaskStatuses
from sqlalchemy.future import select

from cosmos.core.config import core_settings, redis_raw
from cosmos.core.scheduled_tasks.scheduled_email import scheduled_email_by_type
from cosmos.core.scheduled_tasks.scheduler import cron_scheduler
from cosmos.core.scheduled_tasks.task_cleanup import cleanup_old_tasks
from cosmos.db.models import AccountHolderEmail

if TYPE_CHECKING:
    from collections.abc import Callable

    from pytest_mock import MockerFixture
    from retry_tasks_lib.db.models import TaskType
    from sqlalchemy.orm import Session

    from cosmos.db.models import AccountHolder, Campaign, CampaignBalance, EmailTemplate
    from tests.conftest import SetupType


def test_cleanup_old_tasks(
    db_session: "Session", create_mock_task: "Callable[..., RetryTask]", mocker: "MockerFixture"
) -> None:

    now = datetime.now(tz=UTC)

    deletable_task = create_mock_task()
    deletable_task.status = RetryTaskStatuses.SUCCESS
    deletable_task.created_at = now - timedelta(days=181)
    deleted_task_id = deletable_task.retry_task_id

    wrong_status_task = create_mock_task()
    wrong_status_task.status = RetryTaskStatuses.FAILED
    wrong_status_task.created_at = now - timedelta(days=200)

    not_old_enough_task = create_mock_task()
    not_old_enough_task.status = RetryTaskStatuses.SUCCESS
    not_old_enough_task.created_at = now - timedelta(days=10)

    db_session.commit()

    mock_logger = mocker.patch("cosmos.core.scheduled_tasks.task_cleanup.logger")

    cleanup_old_tasks()

    logger_calls = mock_logger.info.call_args_list

    assert logger_calls[0].args == ("Cleaning up tasks created before %s...", (now - timedelta(days=6 * 30)).date())
    assert logger_calls[1].args == ("Deleted %d tasks. ( °╭ ︿ ╮°)", 1)

    db_session.expire_all()

    assert not db_session.get(RetryTask, deleted_task_id)
    assert wrong_status_task.retry_task_id
    assert not_old_enough_task.retry_task_id


def test_scheduled_email_by_type(
    setup: "SetupType",
    mocker: "MockerFixture",
    create_account_holder: "Callable[..., AccountHolder]",
    create_balance: "Callable[..., CampaignBalance]",
    send_email_task_type: "TaskType",
    balance_reset_email_template: "EmailTemplate",
) -> None:
    redis_raw.delete(f"{core_settings.REDIS_KEY_PREFIX}{cron_scheduler.name}:{scheduled_email_by_type.__qualname__}")

    db_session, retailer, eligible_account_holder_1 = setup
    eligible_account_holder_2 = create_account_holder(email="test@account.2")
    non_eligible_account_holder_1 = create_account_holder(email="test@account.3")
    non_eligible_account_holder_2 = create_account_holder(email="test@account.4")
    non_eligible_account_holder_3 = create_account_holder(email="test@account.5")

    retailer.balance_lifespan = 30
    retailer.balance_reset_advanced_warning_days = 5

    eligible_balance_1 = create_balance(
        account_holder_id=eligible_account_holder_1.id,
        balance=700,
        reset_date=datetime.now(tz=UTC) + timedelta(days=retailer.balance_reset_advanced_warning_days),
    )
    eligible_balance_2 = create_balance(
        account_holder_id=eligible_account_holder_2.id,
        balance=1424,
        reset_date=datetime.now(tz=UTC) + timedelta(days=retailer.balance_reset_advanced_warning_days),
    )
    create_balance(account_holder_id=non_eligible_account_holder_1.id, reset_date=datetime.now(tz=UTC))
    create_balance(
        account_holder_id=non_eligible_account_holder_2.id,
        reset_date=datetime.now(tz=UTC) + timedelta(days=retailer.balance_reset_advanced_warning_days + 1),
    )
    create_balance(
        account_holder_id=non_eligible_account_holder_3.id,
        balance=0,
        reset_date=datetime.now(tz=UTC) + timedelta(days=retailer.balance_reset_advanced_warning_days),
    )

    db_session.commit()

    mock_enqueue = mocker.patch("cosmos.core.scheduled_tasks.scheduled_email.enqueue_many_retry_tasks")
    mock_now = datetime.now(tz=UTC)
    mock_datetime = mocker.patch("cosmos.accounts.send_email_params_gen.datetime")
    mock_datetime.now.return_value = mock_now

    scheduled_email_by_type(email_type_slug="BALANCE_RESET")

    retry_tasks = db_session.scalars(select(RetryTask)).unique().all()
    assert len(retry_tasks) == 2

    mock_enqueue.assert_called_once_with(
        mocker.ANY,
        retry_tasks_ids=[rt.retry_task_id for rt in retry_tasks],
        connection=mocker.ANY,
    )

    task_params_1 = retry_tasks[0].get_params()
    task_params_2 = retry_tasks[1].get_params()

    if task_params_2["account_holder_id"] == eligible_account_holder_1.id:
        task_params_1, task_params_2 = task_params_2, task_params_1

    assert eligible_balance_1.reset_date
    assert eligible_balance_2.reset_date

    assert not DeepDiff(
        task_params_1,
        {
            "account_holder_id": eligible_account_holder_1.id,
            "retailer_id": eligible_account_holder_1.retailer_id,
            "template_type": "BALANCE_RESET",
            "extra_params": {
                "current_balance": "7.00",
                "balance_reset_date": eligible_balance_1.reset_date.strftime("%d/%m/%Y"),
                "datetime": mock_now.strftime("%H:%M %d/%m/%Y"),
                "campaign_slug": eligible_balance_1.campaign.slug,
                "retailer_slug": eligible_account_holder_1.retailer.slug,
                "retailer_name": eligible_account_holder_1.retailer.name,
            },
        },
    )

    assert not DeepDiff(
        task_params_2,
        {
            "account_holder_id": eligible_account_holder_2.id,
            "retailer_id": eligible_account_holder_2.retailer_id,
            "template_type": "BALANCE_RESET",
            "extra_params": {
                "current_balance": "14.24",
                "balance_reset_date": eligible_balance_2.reset_date.strftime("%d/%m/%Y"),
                "datetime": mock_now.strftime("%H:%M %d/%m/%Y"),
                "campaign_slug": eligible_balance_2.campaign.slug,
                "retailer_slug": eligible_account_holder_2.retailer.slug,
                "retailer_name": eligible_account_holder_2.retailer.name,
            },
        },
    )


def test_scheduled_email_by_type_mail_already_sent(
    setup: "SetupType",
    mocker: "MockerFixture",
    create_account_holder: "Callable[..., AccountHolder]",
    create_balance: "Callable[..., CampaignBalance]",
    send_email_task_type: "TaskType",
    balance_reset_email_template: "EmailTemplate",
    create_campaign: "Callable[..., Campaign]",
) -> None:
    redis_raw.delete(f"{core_settings.REDIS_KEY_PREFIX}{cron_scheduler.name}:{scheduled_email_by_type.__qualname__}")

    db_session, retailer, eligible_account_holder = setup
    non_eligible_account_holder = create_account_holder(email="test@account.2")

    campaign_1 = create_campaign(slug="campaign-1")
    campaign_2 = create_campaign(slug="campaign-2")

    retailer.balance_lifespan = 30
    retailer.balance_reset_advanced_warning_days = 5

    eligible_balance = create_balance(
        account_holder_id=eligible_account_holder.id,
        campaign_id=campaign_1.id,
        balance=700,
        reset_date=datetime.now(tz=UTC) + timedelta(days=retailer.balance_reset_advanced_warning_days),
    )
    non_eligible_balance_1 = create_balance(
        account_holder_id=eligible_account_holder.id,
        campaign_id=campaign_2.id,
        balance=1424,
        reset_date=datetime.now(tz=UTC) + timedelta(days=retailer.balance_reset_advanced_warning_days),
    )
    non_eligible_balance_2 = create_balance(
        account_holder_id=non_eligible_account_holder.id,
        campaign_id=campaign_2.id,
        balance=1424,
        reset_date=datetime.now(tz=UTC) + timedelta(days=retailer.balance_reset_advanced_warning_days),
    )

    account_holder_email_1 = AccountHolderEmail(
        account_holder_id=eligible_account_holder.id,
        email_type_id=balance_reset_email_template.email_type_id,
        campaign_id=non_eligible_balance_1.campaign_id,
    )
    account_holder_email_2 = AccountHolderEmail(
        account_holder_id=non_eligible_account_holder.id,
        email_type_id=balance_reset_email_template.email_type_id,
        campaign_id=non_eligible_balance_2.campaign_id,
    )
    db_session.add(account_holder_email_1)
    db_session.add(account_holder_email_2)

    db_session.commit()

    mock_enqueue = mocker.patch("cosmos.core.scheduled_tasks.scheduled_email.enqueue_many_retry_tasks")
    mock_now = datetime.now(tz=UTC)
    mock_datetime = mocker.patch("cosmos.accounts.send_email_params_gen.datetime")
    mock_datetime.now.return_value = mock_now

    scheduled_email_by_type(email_type_slug="BALANCE_RESET")

    retry_tasks = db_session.scalars(select(RetryTask)).unique().all()
    assert len(retry_tasks) == 1

    retry_task = retry_tasks[0]

    mock_enqueue.assert_called_once_with(
        mocker.ANY,
        retry_tasks_ids=[retry_task.retry_task_id],
        connection=mocker.ANY,
    )

    assert eligible_balance.reset_date
    assert not DeepDiff(
        retry_task.get_params(),
        {
            "account_holder_id": eligible_account_holder.id,
            "retailer_id": eligible_account_holder.retailer_id,
            "template_type": "BALANCE_RESET",
            "extra_params": {
                "current_balance": "7.00",
                "balance_reset_date": eligible_balance.reset_date.strftime("%d/%m/%Y"),
                "datetime": mock_now.strftime("%H:%M %d/%m/%Y"),
                "campaign_slug": eligible_balance.campaign.slug,
                "retailer_slug": eligible_account_holder.retailer.slug,
                "retailer_name": eligible_account_holder.retailer.name,
            },
        },
    )
