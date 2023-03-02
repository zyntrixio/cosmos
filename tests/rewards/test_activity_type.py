import uuid

from datetime import datetime, timezone

import pytest

from pytest_mock import MockerFixture

from cosmos.rewards.activity.enums import ActivityType


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
    fake_now = datetime.now(tz=timezone.utc)
    mock_datetime.now.return_value = fake_now

    account_holder_uuid = str(uuid.uuid4())
    activity_datetime = datetime.now(tz=timezone.utc)
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
    fake_now = datetime.now(tz=timezone.utc)
    mock_datetime.now.return_value = fake_now

    account_holder_uuid = str(uuid.uuid4())
    activity_datetime = datetime.now(tz=timezone.utc)
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
