from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

from retry_tasks_lib.db.models import RetryTask

from cosmos.rewards.config import reward_settings
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


@pytest.mark.parametrize(
    ["validity_days", "expiry_date", "expected_expiry_date"],
    (
        pytest.param(
            15,
            None,
            "from_validity_days",
            id="validity days is set, no existing expiry date,  expiry_date calculated from validity days",
        ),
        pytest.param(
            15,
            datetime.now(tz=UTC) + timedelta(days=15),
            "from_expiry_date",
            id="validity_days is set, expiry date is set, existing expiry_date has priority",
        ),
        pytest.param(
            0,
            datetime.now(tz=UTC) + timedelta(days=15),
            "from_expiry_date",
            id="no validity_days, expiry date is set, existing expiry_date has priority",
        ),
        pytest.param(0, None, "error", id="no validity_days, no existing expiry date, an error is raised"),
    ),
)
def test_get_allocable_reward_ok(
    validity_days: int,
    expiry_date: datetime | None,
    expected_expiry_date: str,
    mocker: "MockerFixture",
    setup_rewards: "RewardsSetupType",
    campaign_with_rules: "Campaign",
    account_holder: "AccountHolder",
    pre_loaded_reward_issuance_task: "RetryTask",
    mock_issued_reward_activity: MagicMock,
) -> None:
    db_session, reward_config, reward = setup_rewards

    reward.expiry_date = expiry_date
    reward_config.required_fields_values = f"validity_days: {validity_days}"
    db_session.commit()

    now = datetime.now(tz=UTC)

    mock_datetime = mocker.patch("cosmos.rewards.fetch_reward.pre_loaded.datetime")
    mock_datetime.now.return_value = now

    def issue_reward() -> bool:
        return issue_agent_specific_reward(
            db_session,
            campaign=campaign_with_rules,
            reward_config=reward_config,
            account_holder=account_holder,
            retry_task=pre_loaded_reward_issuance_task,
            task_params=IssuanceTaskParams(**pre_loaded_reward_issuance_task.get_params()),
        )

    if expected_expiry_date == "error":
        with pytest.raises(ValueError) as exc_info:
            issue_reward()

        assert exc_info.value.args[0] == "Both validity_days and expiry_date are None"

        db_session.refresh(reward)
        assert reward.account_holder_id is None
        assert reward.campaign_id is None
        assert reward.issued_date is None
        assert not reward.associated_url

    else:
        success = issue_reward()
        assert success
        db_session.refresh(reward)
        assert reward.account_holder_id == account_holder.id
        assert reward.campaign_id == campaign_with_rules.id
        assert reward.issued_date == now.replace(tzinfo=None)
        assert (
            reward.associated_url
            == f"{reward_settings.PRE_LOADED_REWARD_BASE_URL}/reward?retailer={reward_config.retailer.slug}&"
            f"reward={reward.reward_uuid}"
        )

        match expected_expiry_date:
            case "from_validity_days":
                assert reward.expiry_date == (now + timedelta(days=validity_days)).replace(tzinfo=None)
            case "from_expiry_date":
                assert expiry_date
                assert reward.expiry_date == expiry_date.replace(tzinfo=None)

        mock_issued_reward_activity.assert_called()
