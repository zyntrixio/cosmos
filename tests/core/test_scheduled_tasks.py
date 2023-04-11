from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from deepdiff import DeepDiff
from retry_tasks_lib.db.models import RetryTask
from sqlalchemy.future import select

from cosmos.core.config import core_settings, redis_raw
from cosmos.core.scheduled_tasks.scheduled_email import scheduled_email_by_type
from cosmos.core.scheduled_tasks.scheduler import cron_scheduler
from cosmos.db.models import AccountHolderEmail

if TYPE_CHECKING:
    from collections.abc import Callable

    from pytest_mock import MockerFixture
    from retry_tasks_lib.db.models import TaskType

    from cosmos.db.models import AccountHolder, Campaign, CampaignBalance, EmailTemplate, EmailType, Transaction
    from tests.conftest import SetupType


def test_scheduled_email_by_type__balance_reset(
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


def test_scheduled_email_by_type_mail_already_sent__balance_reset(
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


def test_scheduled_email_by_type__purchase_prompt(
    setup: "SetupType",
    mocker: "MockerFixture",
    create_account_holder: "Callable[..., AccountHolder]",
    create_transaction: "Callable[..., Transaction]",
    send_email_task_type: "TaskType",
    purchase_prompt_email_type: "EmailType",
    purchase_prompt_email_template: "EmailTemplate",
    campaign: "Campaign",
) -> None:
    redis_raw.delete(f"{core_settings.REDIS_KEY_PREFIX}{cron_scheduler.name}:{scheduled_email_by_type.__qualname__}")

    db_session, _, account_holder_1 = setup

    mock_now = datetime.now(tz=UTC)

    # Should get notification:
    account_holder_1.created_at = mock_now - timedelta(days=10)

    # refund transaction within prompt days
    account_holder_2 = create_account_holder(email="test@account.2", created_at=mock_now - timedelta(days=10))
    create_transaction(
        account_holder=account_holder_2,
        **{
            "datetime": mock_now - timedelta(days=1),
            "transaction_id": "ah2_tx1",
            "mid": "amid",
            "amount": -100,
            "processed": True,
        },
    )

    # Should NOT get notification
    # not 10 days since joining
    account_holder_3 = create_account_holder(email="test@account.3", created_at=mock_now - timedelta(days=9))

    # purchase transaction within prompt days
    account_holder_4 = create_account_holder(email="test@account.4", created_at=mock_now - timedelta(days=10))
    create_transaction(
        account_holder=account_holder_4,
        **{
            "datetime": mock_now - timedelta(days=1),
            "transaction_id": "ah4_tx1",
            "mid": "amid",
            "amount": 100,
            "processed": True,
        },
    )

    db_session.commit()

    mock_enqueue = mocker.patch("cosmos.core.scheduled_tasks.scheduled_email.enqueue_many_retry_tasks")
    mock_datetime = mocker.patch("cosmos.accounts.send_email_params_gen.datetime")
    mock_datetime.now.return_value = mock_now

    scheduled_email_by_type(email_type_slug="PURCHASE_PROMPT")

    retry_tasks = db_session.scalars(select(RetryTask)).unique().all()
    assert len(retry_tasks) == 2

    mock_enqueue.assert_called_once_with(
        mocker.ANY,
        retry_tasks_ids=[task.retry_task_id for task in retry_tasks],
        connection=mocker.ANY,
    )

    task_1 = retry_tasks[0]
    assert task_1.get_params() == {
        "account_holder_id": account_holder_1.id,
        "retailer_id": account_holder_1.retailer_id,
        "template_type": "PURCHASE_PROMPT",
    }
    task_2 = retry_tasks[1]
    assert task_2.get_params() == {
        "account_holder_id": account_holder_2.id,
        "retailer_id": account_holder_2.retailer_id,
        "template_type": "PURCHASE_PROMPT",
    }

    # Add AccountHolderEmails to simulate emails already sent for account_holder_1 and account_holder_2
    for account_holder, task in ((account_holder_1, task_1), (account_holder_2, task_2)):
        ahe = AccountHolderEmail(
            account_holder_id=account_holder.id,
            email_type_id=purchase_prompt_email_type.id,
            retry_task_id=task.retry_task_id,
            campaign_id=campaign.id,
        )
        db_session.add(ahe)

    # now move account holder 3 into scope and make sure it's the only new task
    account_holder_3.created_at = mock_now - timedelta(days=12)

    db_session.commit()

    scheduled_email_by_type(email_type_slug="PURCHASE_PROMPT")

    retry_tasks = (
        db_session.scalars(
            select(RetryTask).where(RetryTask.retry_task_id.not_in([task_1.retry_task_id, task_2.retry_task_id]))
        )
        .unique()
        .all()
    )

    assert len(retry_tasks) == 1

    task_3 = retry_tasks[0]
    assert task_3.get_params() == {
        "account_holder_id": account_holder_3.id,
        "retailer_id": account_holder_3.retailer_id,
        "template_type": "PURCHASE_PROMPT",
    }
