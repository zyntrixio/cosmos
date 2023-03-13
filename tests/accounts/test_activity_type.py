import uuid

from datetime import UTC, datetime

from pytest_mock import MockerFixture

from cosmos.accounts.activity.enums import ActivityType


def test_get_balance_change_activity_data(mocker: MockerFixture) -> None:
    mock_datetime = mocker.patch("cosmos.core.activity.enums.datetime")
    fake_now = datetime.now(tz=UTC)
    mock_datetime.now.return_value = fake_now

    account_holder_uuid = str(uuid.uuid4())
    activity_datetime = datetime.now(tz=UTC)

    payload = ActivityType.get_balance_change_activity_data(
        account_holder_uuid=account_holder_uuid,
        retailer_slug="test-retailer",
        summary="This is a summary",
        new_balance=1000,
        campaigns=["campaign-slug"],
        reason="A very good reason",
        activity_datetime=activity_datetime,
        original_balance=500,
    )
    assert payload == {
        "type": ActivityType.BALANCE_CHANGE.name,
        "datetime": fake_now,
        "underlying_datetime": activity_datetime,
        "summary": "This is a summary",
        "reasons": ["A very good reason"],
        "activity_identifier": "N/A",
        "user_id": account_holder_uuid,
        "associated_value": "1000",
        "retailer": "test-retailer",
        "campaigns": ["campaign-slug"],
        "data": {
            "new_balance": 1000,
            "original_balance": 500,
        },
    }
