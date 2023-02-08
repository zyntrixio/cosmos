from typing import TYPE_CHECKING, Callable, Generator, NamedTuple

import pytest

from pytest_mock import MockerFixture
from retry_tasks_lib.db.models import RetryTask, TaskType, TaskTypeKey, TaskTypeKeyValue

from cosmos.core.config import settings
from cosmos.db.models import RetailerFetchType, Reward, RewardConfig
from cosmos.rewards.activity.enums import IssuedRewardReasons
from cosmos.rewards.fetch_reward.base import BaseAgent

if TYPE_CHECKING:
    from unittest.mock import MagicMock

    from sqlalchemy.orm import Session

    from cosmos.db.models import AccountHolder, Campaign


class RewardsSetupType(NamedTuple):
    db_session: "Session"
    reward_config: RewardConfig
    reward: Reward


@pytest.fixture(scope="function")
def setup_rewards(
    db_session: "Session", reward_config: RewardConfig, reward: Reward
) -> Generator[RewardsSetupType, None, None]:
    yield RewardsSetupType(db_session, reward_config, reward)


@pytest.fixture(scope="function")
def reward_issuance_task_type(db_session: "Session") -> TaskType:
    tt = TaskType(
        name=settings.REWARD_ISSUANCE_TASK_NAME,
        path="cosmos.rewards.tasks.issuance.issue_reward",
        error_handler_path="cosmos.core.tasks.error_handlers.default_handler",
        queue_name="cosmos:default",
    )
    db_session.add(tt)
    db_session.flush()
    db_session.bulk_save_objects(
        [
            TaskTypeKey(task_type_id=tt.task_type_id, name=key_name, type=key_type)
            for key_name, key_type in (
                ("campaign_id", "INTEGER"),
                ("account_holder_id", "INTEGER"),
                ("reward_config_id", "INTEGER"),
                ("pending_reward_id", "STRING"),
                ("reason", "STRING"),
                ("agent_state_params_raw", "STRING"),
            )
        ]
    )
    db_session.commit()
    return tt


@pytest.fixture(scope="function")
def jigsaw_reward_config(db_session: "Session", jigsaw_retailer_fetch_type: RetailerFetchType) -> RewardConfig:
    config = RewardConfig(
        slug="test-jigsaw-reward",
        required_fields_values="transaction_value: 15",
        retailer_id=jigsaw_retailer_fetch_type.retailer_id,
        fetch_type_id=jigsaw_retailer_fetch_type.fetch_type_id,
        active=True,
    )
    db_session.add(config)
    db_session.commit()
    return config


@pytest.fixture(scope="function")
def jigsaw_campaign(
    db_session: "Session", campaign_with_rules: "Campaign", jigsaw_reward_config: RewardConfig
) -> "Campaign":
    campaign_with_rules.retailer_id = jigsaw_reward_config.retailer_id
    db_session.commit()
    return campaign_with_rules


@pytest.fixture(scope="function")
def create_reward_issuance_task(
    db_session: "Session",
    jigsaw_reward_config: RewardConfig,
    reward_issuance_task_type: TaskType,
    account_holder: "AccountHolder",
    jigsaw_campaign: "Campaign",
) -> Callable[..., RetryTask]:

    payload: dict[str, str | int] = {
        "campaign_id": jigsaw_campaign.id,
        "account_holder_id": account_holder.id,
        "reward_config_id": jigsaw_reward_config.id,
        "reason": IssuedRewardReasons.GOAL_MET.name,
    }

    def _create_task(**update_values: int | str) -> RetryTask:

        payload.update(update_values)

        rt = RetryTask(task_type_id=reward_issuance_task_type.task_type_id)
        db_session.add(rt)
        db_session.flush()
        key_ids = reward_issuance_task_type.get_key_ids_by_name()
        db_session.bulk_save_objects(
            [
                TaskTypeKeyValue(
                    task_type_key_id=key_ids[key_name],
                    value=value,
                    retry_task_id=rt.retry_task_id,
                )
                for key_name, value in payload.items()
            ]
        )
        db_session.commit()
        return rt

    return _create_task


@pytest.fixture(scope="function")
def jigsaw_reward_issuance_task(create_reward_issuance_task: Callable[..., RetryTask]) -> RetryTask:
    return create_reward_issuance_task()


@pytest.fixture(scope="function")
def pre_loaded_reward_issuance_task(
    create_reward_issuance_task: Callable[..., RetryTask],
    pre_loaded_retailer_fetch_type: "RetailerFetchType",
    reward_config: "RewardConfig",
    campaign_with_rules: "Campaign",
) -> RetryTask:

    return create_reward_issuance_task(
        campaign_id=campaign_with_rules.id,
        reward_config_id=reward_config.id,
    )


@pytest.fixture(scope="function")
def mock_issued_reward_activity(mocker: MockerFixture) -> "MagicMock":
    return mocker.patch.object(BaseAgent, "_send_issued_reward_activity")
