from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

from retry_tasks_lib.db.models import RetryTask

from cosmos.core.config import settings
from cosmos.rewards.fetch_reward import issue_agent_specific_reward
from cosmos.rewards.fetch_reward.base import BaseAgent
from cosmos.rewards.schemas import IssuanceTaskParams

if TYPE_CHECKING:  # pragma: no cover

    from pytest_mock import MockerFixture

    from cosmos.db.models import AccountHolder, Campaign
    from tests.rewards.conftest import RewardsSetupType


def test_get_allocable_reward_wrong_path(
    mocker: "MockerFixture",
    setup_rewards: "RewardsSetupType",
) -> None:
    db_session, reward_config, _ = setup_rewards

    reward_config.fetch_type.path = "wrong.Path"
    db_session.commit()

    spy_logger = mocker.spy(BaseAgent, "logger")

    MagicMock(spec=RetryTask)

    with pytest.raises(ModuleNotFoundError):
        issue_agent_specific_reward(
            db_session,
            campaign=MagicMock(),
            reward_config=reward_config,
            account_holder=MagicMock(),
            retry_task=MagicMock(spec=RetryTask),
            task_params=MagicMock(spec=IssuanceTaskParams),
        )

    spy_logger.warning.assert_called_once()


def test_get_allocable_reward_ok(
    mocker: "MockerFixture",
    setup_rewards: "RewardsSetupType",
    campaign_with_rules: "Campaign",
    account_holder: "AccountHolder",
    pre_loaded_reward_issuance_task: "RetryTask",
    mock_issued_reward_activity: MagicMock,
) -> None:
    db_session, reward_config, reward = setup_rewards
    now = datetime.now(tz=timezone.utc)
    validity_days = reward_config.load_required_fields_values()["validity_days"]
    mock_datetime = mocker.patch("cosmos.rewards.fetch_reward.pre_loaded.datetime")
    mock_datetime.now.return_value = now

    success = issue_agent_specific_reward(
        db_session,
        campaign=campaign_with_rules,
        reward_config=reward_config,
        account_holder=account_holder,
        retry_task=pre_loaded_reward_issuance_task,
        task_params=IssuanceTaskParams(**pre_loaded_reward_issuance_task.get_params()),
    )

    assert success
    db_session.refresh(reward)
    assert reward.account_holder_id == account_holder.id
    assert reward.issued_date == now.replace(tzinfo=None)
    assert reward.expiry_date == (now + timedelta(days=validity_days)).replace(tzinfo=None)
    assert (
        reward.associated_url == f"{settings.PRE_LOADED_REWARD_BASE_URL}/reward?retailer={reward_config.retailer.slug}&"
        f"reward={reward.reward_uuid}"
    )

    mock_issued_reward_activity.assert_called()
