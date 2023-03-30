import uuid

from collections.abc import Callable
from datetime import UTC, datetime

import pytest

from pytest_mock import MockerFixture

from cosmos.db.models import Campaign
from cosmos.rewards.activity.enums import ActivityType, IssuedRewardReasons
from tests.conftest import SetupType


@pytest.mark.parametrize(
    ("original_status", "new_status", "count"),
    (
        pytest.param(None, "PENDING", 2),
        pytest.param(None, "PENDING", None),
        pytest.param("PENDING", "ISSUED", 2),
        pytest.param("PENDING", "ISSUED", None),
    ),
)
def test_get_reward_status_activity_data(
    new_status: str, original_status: str | None, count: int | None, mocker: MockerFixture
) -> None:
    mock_datetime = mocker.patch("cosmos.core.activity.enums.datetime")
    fake_now = datetime.now(tz=UTC)
    mock_datetime.now.return_value = fake_now

    account_holder_uuid = str(uuid.uuid4())
    activity_datetime = datetime.now(tz=UTC)
    pending_reward_uuid = str(uuid.uuid4())

    payload = ActivityType.get_reward_status_activity_data(
        account_holder_uuid=account_holder_uuid,
        retailer_slug="test-retailer",
        summary="A summary",
        new_status=new_status,
        campaigns=["campaign-a"],
        reason="A very good reason",
        activity_datetime=activity_datetime,
        original_status=original_status,
        activity_identifier=pending_reward_uuid,
        count=count,
    )

    assert payload == {
        "type": ActivityType.REWARD_STATUS.name,
        "datetime": fake_now,
        "underlying_datetime": activity_datetime,
        "summary": "A summary",
        "reasons": ["A very good reason"],
        "activity_identifier": pending_reward_uuid,
        "user_id": account_holder_uuid,
        "associated_value": new_status,
        "retailer": "test-retailer",
        "campaigns": ["campaign-a"],
        "data": {
            "new_status": new_status,
        }
        | ({"count": count} if count else {})
        | ({"original_status": original_status} if original_status else {}),
    }


def test_get_reward_update_activity_data(mocker: MockerFixture) -> None:
    mock_datetime = mocker.patch("cosmos.core.activity.enums.datetime")
    fake_now = datetime.now(tz=UTC)
    mock_datetime.now.return_value = fake_now

    account_holder_uuid = str(uuid.uuid4())
    activity_datetime = datetime.now(tz=UTC)
    payload = ActivityType.get_reward_update_activity_data(
        account_holder_uuid=account_holder_uuid,
        retailer_slug="test-retailer",
        summary="A summary",
        campaigns=["campaign-a"],
        reason="A very good reason",
        activity_datetime=activity_datetime,
        activity_identifier="activity-id",
        reward_update_data={"original_total_cost_to_user": 100, "new_total_cost_to_user": 500},
    )
    new_total_cost_to_user = 500
    associated_value = f"£{new_total_cost_to_user/100:.2f}"
    assert payload == {
        "activity_identifier": "activity-id",
        "associated_value": associated_value,
        "campaigns": ["campaign-a"],
        "data": {"new_total_cost_to_user": 500, "original_total_cost_to_user": 100},
        "datetime": fake_now,
        "reasons": ["A very good reason"],
        "retailer": "test-retailer",
        "summary": "A summary",
        "type": "REWARD_UPDATE",
        "underlying_datetime": activity_datetime,
        "user_id": account_holder_uuid,
    }


def test_get_pending_reward_deleted_activity_data(mocker: MockerFixture) -> None:
    fake_now = datetime.now(tz=UTC)
    mock_datetime = mocker.patch("cosmos.core.activity.enums.datetime")
    mock_datetime.now.return_value = fake_now
    account_holder_uuid = str(uuid.uuid4())
    pending_reward_uuid = str(uuid.uuid4())

    payload = ActivityType.get_pending_reward_deleted_activity_data(
        retailer_slug="test-retailer",
        campaign_slug="test-campaign",
        account_holder_uuid=account_holder_uuid,
        pending_reward_uuid=pending_reward_uuid,
        activity_datetime=fake_now,
    )
    assert payload == {
        "type": ActivityType.REWARD_STATUS.name,
        "datetime": fake_now,
        "underlying_datetime": fake_now,
        "summary": "test-retailer Pending Reward removed for test-campaign",
        "reasons": ["Pending Reward removed due to campaign end/cancellation"],
        "activity_identifier": pending_reward_uuid,
        "user_id": account_holder_uuid,
        "associated_value": "Deleted",
        "retailer": "test-retailer",
        "campaigns": ["test-campaign"],
        "data": {
            "new_status": "deleted",
            "original_status": "pending",
        },
    }


def test_get_pending_reward_transferred_activity_data(mocker: MockerFixture) -> None:
    fake_now = datetime.now(tz=UTC)
    mock_datetime = mocker.patch("cosmos.core.activity.enums.datetime")
    mock_datetime.now.return_value = fake_now
    account_holder_uuid = str(uuid.uuid4())
    pending_reward_uuid = str(uuid.uuid4())

    payload = ActivityType.get_pending_reward_transferred_activity_data(
        retailer_slug="test-retailer",
        from_campaign_slug="test-campaign",
        to_campaign_slug="new-test-campaign",
        account_holder_uuid=account_holder_uuid,
        activity_datetime=fake_now,
        pending_reward_uuid=pending_reward_uuid,
    )
    assert payload == {
        "type": ActivityType.REWARD_STATUS.name,
        "datetime": fake_now,
        "underlying_datetime": fake_now,
        "summary": "test-retailer pending reward transferred from test-campaign to new-test-campaign",
        "reasons": ["Pending reward transferred at campaign end"],
        "activity_identifier": pending_reward_uuid,
        "user_id": account_holder_uuid,
        "associated_value": "N/A",
        "retailer": "test-retailer",
        "campaigns": ["test-campaign", "new-test-campaign"],
        "data": {
            "new_campaign": "new-test-campaign",
            "old_campaign": "test-campaign",
        },
    }


@pytest.mark.parametrize(
    "params",
    [
        [
            IssuedRewardReasons.CAMPAIGN_END,
            True,
            True,
            False,
            "Test Retailer Pending Reward issued for Test Campaign",
            True,
        ],
        [IssuedRewardReasons.CAMPAIGN_END, True, False, True, None, False],
        [
            IssuedRewardReasons.CONVERTED,
            True,
            True,
            False,
            "Test Retailer Pending Reward issued for Test Campaign",
            True,
        ],
        [IssuedRewardReasons.CONVERTED, False, True, True, None, False],
        [IssuedRewardReasons.GOAL_MET, True, True, False, "Test Retailer Reward issued", False],
    ],
)
def test_get_issued_reward_status_activity_data(
    mocker: MockerFixture,
    setup: SetupType,
    create_campaign: Callable[..., "Campaign"],
    params: list,
) -> None:
    reason, campaign, pending_reward, error, summary, new_data_payload = params
    mock_campaign = create_campaign(slug="test-campaign", name="Test Campaign") if campaign else None
    fake_now = datetime.now(tz=UTC)
    mock_datetime = mocker.patch("cosmos.core.activity.enums.datetime")
    mock_datetime.now.return_value = fake_now
    account_holder_uuid = str(uuid.uuid4())
    pending_reward_uuid = str(uuid.uuid4()) if pending_reward else None
    reward_uuid = str(uuid.uuid4())
    retailer = setup.retailer
    if new_data_payload:
        data_payload = {
            "new_status": "issued",
            "reward_slug": "test-reward",
            "original_status": "pending",
            "pending_reward_uuid": pending_reward_uuid,
        }
    else:
        data_payload = {"new_status": "issued", "reward_slug": "test-reward"}

    if not error:
        payload = ActivityType.get_issued_reward_status_activity_data(
            account_holder_uuid=account_holder_uuid,
            retailer=retailer,
            reward_slug="test-reward",
            activity_timestamp=fake_now,
            reward_uuid=reward_uuid,
            pending_reward_uuid=pending_reward_uuid,
            campaign=mock_campaign,
            reason=reason,
        )
        assert payload == {
            "type": ActivityType.REWARD_STATUS.name,
            "datetime": fake_now,
            "underlying_datetime": fake_now,
            "summary": summary,
            "reasons": [reason.value],
            "activity_identifier": reward_uuid,
            "user_id": account_holder_uuid,
            "associated_value": "issued",
            "retailer": "re-test",
            "campaigns": ["test-campaign"],
            "data": data_payload,
        }
    else:
        with pytest.raises(ValueError) as exc_info:
            ActivityType.get_issued_reward_status_activity_data(
                account_holder_uuid=account_holder_uuid,
                retailer=retailer,
                reward_slug="test-reward",
                activity_timestamp=fake_now,
                reward_uuid=reward_uuid,
                pending_reward_uuid=pending_reward_uuid,
                campaign=mock_campaign,
                reason=reason,
            )

            assert exc_info.value.args[0] == "Pending reward conversion requires a campaign and pending_reward_uuid"
