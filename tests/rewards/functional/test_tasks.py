from datetime import UTC, datetime
from typing import TYPE_CHECKING

import httpretty
import pytest

from deepdiff import DeepDiff
from retry_tasks_lib.db.models import RetryTask, TaskType
from retry_tasks_lib.enums import RetryTaskStatuses
from retry_tasks_lib.utils.synchronous import IncorrectRetryTaskStatusError

from cosmos.campaigns.enums import CampaignStatuses
from cosmos.rewards.config import reward_settings
from cosmos.rewards.tasks.issuance import issue_reward

if TYPE_CHECKING:  # pragma: no cover
    from unittest.mock import MagicMock

    from pytest_mock import MockerFixture

    from cosmos.db.models import AccountHolder, Campaign
    from tests.rewards.conftest import RewardsSetupType


@pytest.fixture(scope="function")
def mock_issued_reward_email_enqueue(send_email_task_type: "TaskType", mocker: "MockerFixture") -> "MagicMock":
    # send_email_task_type param is needed
    return mocker.patch("cosmos.rewards.tasks.issuance.enqueue_retry_task")


@httpretty.activate
def test_reward_issuance_ok(
    setup_rewards: "RewardsSetupType",
    campaign_with_rules: "Campaign",
    account_holder: "AccountHolder",
    pre_loaded_reward_issuance_task: "RetryTask",
    mock_issued_reward_activity: "MagicMock",
    mock_issued_reward_email_enqueue: "MagicMock",
) -> None:
    db_session, _, reward = setup_rewards

    assert reward.account_holder_id is None
    assert reward.issued_date is None
    assert not reward.associated_url

    issue_reward(pre_loaded_reward_issuance_task.retry_task_id)

    db_session.refresh(pre_loaded_reward_issuance_task)
    assert pre_loaded_reward_issuance_task.attempts == 1
    assert pre_loaded_reward_issuance_task.next_attempt_time is None
    assert pre_loaded_reward_issuance_task.status == RetryTaskStatuses.SUCCESS

    db_session.refresh(reward)
    assert reward.account_holder_id == account_holder.id
    assert reward.issued_date is not None
    assert reward.associated_url

    mock_issued_reward_activity.assert_called_once()

    mock_issued_reward_email_enqueue.assert_called_once()
    send_email_task: RetryTask = mock_issued_reward_email_enqueue.call_args.kwargs.get("retry_task")
    assert send_email_task
    db_session.add(send_email_task)

    assert not DeepDiff(
        send_email_task.get_params(),
        {
            "retailer_id": account_holder.retailer_id,
            "template_type": "REWARD_ISSUANCE",
            "account_holder_id": account_holder.id,
            "extra_params": {
                "reward_url": reward.associated_url,
                "campaign_slug": campaign_with_rules.slug,
                "retailer_slug": account_holder.retailer.slug,
                "retailer_name": account_holder.retailer.name,
                "account_holder_uuid": str(account_holder.account_holder_uuid),
            },
        },
    )


def test_reward_issuance_wrong_status(
    setup_rewards: "RewardsSetupType",
    campaign_with_rules: "Campaign",
    pre_loaded_reward_issuance_task: "RetryTask",
    mock_issued_reward_email_enqueue: "MagicMock",
) -> None:
    db_session = setup_rewards.db_session

    pre_loaded_reward_issuance_task.status = RetryTaskStatuses.FAILED
    campaign_with_rules.status = CampaignStatuses.ENDED
    db_session.commit()

    with pytest.raises(IncorrectRetryTaskStatusError):
        issue_reward(pre_loaded_reward_issuance_task.retry_task_id)

    db_session.refresh(pre_loaded_reward_issuance_task)

    assert pre_loaded_reward_issuance_task.attempts == 0
    assert pre_loaded_reward_issuance_task.next_attempt_time is None
    assert pre_loaded_reward_issuance_task.status == RetryTaskStatuses.FAILED
    mock_issued_reward_email_enqueue.assert_not_called()


@httpretty.activate
def test_reward_issuance_campaign_is_cancelled(
    setup_rewards: "RewardsSetupType",
    campaign_with_rules: "Campaign",
    pre_loaded_reward_issuance_task: "RetryTask",
    mock_issued_reward_email_enqueue: "MagicMock",
) -> None:
    """
    Test that, if the campaign has been cancelled by the time we get to issue a reward, the issuance is also cancelled.
    """
    db_session = setup_rewards.db_session

    campaign_with_rules.status = CampaignStatuses.CANCELLED
    db_session.commit()

    issue_reward(pre_loaded_reward_issuance_task.retry_task_id)

    db_session.refresh(pre_loaded_reward_issuance_task)

    assert pre_loaded_reward_issuance_task.attempts == 1
    assert pre_loaded_reward_issuance_task.next_attempt_time is None
    assert pre_loaded_reward_issuance_task.status == RetryTaskStatuses.CANCELLED
    mock_issued_reward_email_enqueue.assert_not_called()


@httpretty.activate
def test_reward_issuance_no_reward_and_allocation_is_requeued(
    mocker: "MockerFixture",
    setup_rewards: "RewardsSetupType",
    campaign_with_rules: "Campaign",
    account_holder: "AccountHolder",
    pre_loaded_reward_issuance_task: "RetryTask",
    mock_issued_reward_activity: "MagicMock",
    mock_issued_reward_email_enqueue: "MagicMock",
) -> None:
    """test that no allocable reward results in the allocation being requeued"""
    db_session, _, reward = setup_rewards

    reward.deleted = True
    db_session.commit()

    fake_now = datetime.now(tz=UTC)
    mock_queue = mocker.patch("cosmos.rewards.tasks.issuance.enqueue_retry_task_delay")
    mock_queue.return_value = fake_now

    reward_settings.MESSAGE_IF_NO_PRE_LOADED_REWARDS = True
    mock_sentry = mocker.patch("cosmos.rewards.tasks.issuance.sentry_sdk")

    issue_reward(pre_loaded_reward_issuance_task.retry_task_id)

    db_session.refresh(pre_loaded_reward_issuance_task)
    mock_queue.assert_called_once()
    mock_issued_reward_email_enqueue.assert_not_called()
    mock_sentry.capture_message.assert_called_once()
    assert pre_loaded_reward_issuance_task.attempts == 1
    assert pre_loaded_reward_issuance_task.next_attempt_time is not None
    assert pre_loaded_reward_issuance_task.status == RetryTaskStatuses.WAITING

    # Add new reward and check that it's allocated and marked as allocated
    reward.deleted = False
    db_session.commit()

    # call issue_reward again
    issue_reward(pre_loaded_reward_issuance_task.retry_task_id)

    mock_queue.assert_called_once()  # should not have been called again

    db_session.refresh(pre_loaded_reward_issuance_task)
    assert pre_loaded_reward_issuance_task.attempts == 1
    assert pre_loaded_reward_issuance_task.next_attempt_time is None
    assert pre_loaded_reward_issuance_task.status == RetryTaskStatuses.SUCCESS

    db_session.refresh(reward)
    assert reward.account_holder_id == account_holder.id
    assert reward.issued_date is not None
    assert reward.associated_url

    mock_issued_reward_activity.assert_called_once()
    send_email_task: RetryTask = mock_issued_reward_email_enqueue.call_args.kwargs.get("retry_task")
    assert send_email_task
    db_session.add(send_email_task)

    assert not DeepDiff(
        send_email_task.get_params(),
        {
            "retailer_id": account_holder.retailer_id,
            "template_type": "REWARD_ISSUANCE",
            "account_holder_id": account_holder.id,
            "extra_params": {
                "reward_url": reward.associated_url,
                "campaign_slug": campaign_with_rules.slug,
                "retailer_slug": account_holder.retailer.slug,
                "retailer_name": account_holder.retailer.name,
                "account_holder_uuid": str(account_holder.account_holder_uuid),
            },
        },
    )
