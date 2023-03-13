from collections.abc import Callable, Generator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from unittest.mock import ANY, MagicMock

import pytest

from pytest_mock import MockerFixture
from retry_tasks_lib.db.models import RetryTask, TaskType, TaskTypeKey, TaskTypeKeyValue
from sqlalchemy.future import select

from cosmos.core.config import redis
from cosmos.core.scheduled_tasks.scheduler import cron_scheduler
from cosmos.db.models import PendingReward
from cosmos.rewards.activity.enums import IssuedRewardReasons
from cosmos.rewards.config import reward_settings
from cosmos.rewards.scheduled_tasks.pending_rewards import process_pending_rewards

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from cosmos.db.models import Campaign

redis_lock_key = f"{reward_settings.core.REDIS_KEY_PREFIX}{cron_scheduler.name}:{process_pending_rewards.__qualname__}"


@dataclass
class Mocks:
    logger: MagicMock
    enqueue_many: MagicMock


@pytest.fixture(scope="function")
def mocks(mocker: MockerFixture) -> Mocks:
    return Mocks(
        logger=mocker.patch("cosmos.rewards.scheduled_tasks.pending_rewards.logger"),
        enqueue_many=mocker.patch("cosmos.rewards.scheduled_tasks.pending_rewards.enqueue_many_retry_tasks"),
    )


@pytest.fixture(scope="function", autouse=True)
def delete_leader_lock_key() -> Generator[None, None, None]:
    redis.delete(redis_lock_key)
    yield
    redis.delete(redis_lock_key)


def test_process_pending_rewards(
    mocks: Mocks,
    db_session: "Session",
    reward_issuance_task_type: TaskType,
    pending_reward: PendingReward,
    create_pending_reward: Callable[..., PendingReward],
    campaign_with_rules: "Campaign",
) -> None:
    now = datetime.now(tz=UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    delta = timedelta(days=1)

    pending_reward.campaign_id = campaign_with_rules.id
    pending_reward.conversion_date = now - delta
    future_pending_reward = create_pending_reward(
        campaign_id=campaign_with_rules.id,
        conversion_date=now + delta,
    )
    db_session.commit()

    process_pending_rewards()

    mocks.enqueue_many.assert_called_once()
    tasks = (
        db_session.execute(
            select(RetryTask).where(
                RetryTask.task_type_id == reward_issuance_task_type.task_type_id,
                RetryTask.retry_task_id == TaskTypeKeyValue.retry_task_id,
                TaskTypeKeyValue.value.in_(
                    (
                        str(pending_reward.pending_reward_uuid),
                        str(future_pending_reward.pending_reward_uuid),
                    )
                ),
                TaskTypeKeyValue.task_type_key_id == TaskTypeKey.task_type_key_id,
                TaskTypeKey.name == "pending_reward_uuid",
            )
        )
        .unique()
        .scalars()
        .all()
    )
    assert len(tasks) == 1

    task_params = tasks[0].get_params()
    assert task_params["account_holder_id"] == pending_reward.account_holder_id
    assert task_params["campaign_id"] == pending_reward.campaign_id
    assert task_params["reward_config_id"] == pending_reward.campaign.reward_rule.reward_config_id
    assert task_params["reason"] == IssuedRewardReasons.CONVERTED.name
    assert task_params["pending_reward_uuid"] == str(pending_reward.pending_reward_uuid)


def test_process_pending_rewards_failed_enqueue(
    mocks: Mocks,
    db_session: "Session",
    pending_reward: PendingReward,
    reward_issuance_task_type: TaskType,
    campaign_with_rules: "Campaign",
) -> None:
    pending_reward.campaign_id = campaign_with_rules.id
    pending_reward.conversion_date = datetime.now(tz=UTC) - timedelta(days=1)
    db_session.commit()

    mocks.enqueue_many.side_effect = ValueError("Oops")

    process_pending_rewards()

    mocks.logger.exception.assert_called_once_with(
        "Failed to enqueue %s RetryTasks with ids: %r.",
        reward_settings.REWARD_ISSUANCE_TASK_NAME,
        [ANY],
        exc_info=mocks.enqueue_many.side_effect,
    )


def test_process_pending_rewards_failed_task_creation(
    mocks: Mocks,
    mocker: MockerFixture,
    db_session: "Session",
    pending_reward: PendingReward,
    reward_issuance_task_type: TaskType,
    campaign_with_rules: "Campaign",
) -> None:
    pending_reward.campaign_id = campaign_with_rules.id
    pending_reward.conversion_date = datetime.now(tz=UTC) - timedelta(days=1)
    db_session.commit()

    mock_create_task = mocker.patch("cosmos.rewards.scheduled_tasks.pending_rewards.sync_create_many_tasks")
    mock_create_task.side_effect = ValueError("Oops")

    process_pending_rewards()

    mocks.enqueue_many.assert_not_called()
    mocks.logger.exception.assert_called_once_with(
        "Failed to convert pending rewards.", exc_info=mock_create_task.side_effect
    )


def test_process_pending_rewards_none_to_process(
    mocks: Mocks,
    db_session: "Session",
    reward_issuance_task_type: TaskType,
    pending_reward: PendingReward,
) -> None:
    db_session.delete(pending_reward)
    db_session.commit()

    process_pending_rewards()

    mocks.logger.exception.assert_not_called()
    mocks.enqueue_many.assert_not_called()
