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


def test_get_account_request_activity_data(mocker: MockerFixture) -> None:
    fake_now = datetime.now(tz=UTC)
    mock_datetime = mocker.patch("cosmos.core.activity.enums.datetime")
    mock_datetime.now.return_value = fake_now
    payload = ActivityType.get_account_request_activity_data(
        activity_datetime=fake_now,
        retailer_slug="test-retailer",
        channel="test-channel",
        request_data={
            "credentials": {"email": "test@test.com"},
            "marketing_preferences": [{"key": "marketing_pref", "value": True}],
            "callback_url": "test-url",
            "third_party_identifier": "whatever",
        },
        retailer_profile_config={
            "email": {"required": "true", "label": "email"},
            "first_name": {"required": "true", "label": "first_name"},
            "last_name": {"required": "true", "label": "last_name"},
        },
        result="testresult",
    )
    assert payload == {
        "type": ActivityType.ACCOUNT_REQUEST.name,
        "datetime": fake_now,
        "underlying_datetime": fake_now,
        "summary": "Enrolment Requested for test@test.com",
        "reasons": [],
        "activity_identifier": "N/A",
        "user_id": "whatever",
        "associated_value": "test@test.com",
        "retailer": "test-retailer",
        "campaigns": [],
        "data": {
            "channel": "test-channel",
            "datetime": fake_now,
            "fields": [
                {"field_name": "email", "value": "test@test.com"},
                {"field_name": "marketing_pref", "value": True},
            ],
            "result": "testresult",
        },
    }


def test_get_account_auth_activity_data(mocker: MockerFixture) -> None:
    fake_now = datetime.now(tz=UTC)
    mock_datetime = mocker.patch("cosmos.core.activity.enums.datetime")
    mock_datetime.now.return_value = fake_now
    random_uuid = str(uuid.uuid4())
    payload = ActivityType.get_account_auth_activity_data(
        account_holder_uuid=random_uuid,
        activity_datetime=fake_now,
        retailer_slug="test-retailer",
        channel="test-channel",
    )
    assert payload == {
        "type": ActivityType.ACCOUNT_AUTHENTICATION.name,
        "datetime": fake_now,
        "underlying_datetime": fake_now,
        "summary": "Account added to test-channel",
        "reasons": [],
        "activity_identifier": "N/A",
        "user_id": random_uuid,
        "associated_value": "test-channel",
        "retailer": "test-retailer",
        "campaigns": [],
        "data": {
            "datetime": fake_now,
            "channel": "test-channel",
        },
    }


def test_get_account_enrolment_activity_data(mocker: MockerFixture) -> None:
    fake_now = datetime.now(tz=UTC)
    mock_datetime = mocker.patch("cosmos.core.activity.enums.datetime")
    mock_datetime.now.return_value = fake_now
    random_uuid = str(uuid.uuid4())
    payload = ActivityType.get_account_enrolment_activity_data(
        account_holder_uuid=random_uuid,
        activity_datetime=fake_now,
        retailer_slug="test-retailer",
        channel="test-channel",
        third_party_identifier="test",
    )
    assert payload == {
        "type": ActivityType.ACCOUNT_ENROLMENT.name,
        "datetime": fake_now,
        "underlying_datetime": fake_now,
        "summary": "Joined via test-channel; Account activated",
        "reasons": ["Third Party Identifier: test"],
        "activity_identifier": "test",
        "user_id": random_uuid,
        "associated_value": "test-channel",
        "retailer": "test-retailer",
        "campaigns": [],
        "data": {
            "channel": "test-channel",
            "datetime": fake_now,
        },
    }


def test_get_marketing_preference_change_activity_data(mocker: MockerFixture) -> None:
    fake_now = datetime.now(tz=UTC)
    mock_datetime = mocker.patch("cosmos.core.activity.enums.datetime")
    mock_datetime.now.return_value = fake_now
    random_uuid = str(uuid.uuid4())
    payload = ActivityType.get_marketing_preference_change_activity_data(
        account_holder_uuid=random_uuid,
        retailer_slug="test-retailer",
        activity_datetime=fake_now,
        summary="Unsubscribed via marketing opt-out",
        associated_value="Marketing Preferences unsubscribed",
        field_name="test",
        original_value="test",
        new_value="testing",
    )
    assert payload == {
        "type": ActivityType.ACCOUNT_CHANGE.name,
        "datetime": fake_now,
        "underlying_datetime": fake_now,
        "summary": "Unsubscribed via marketing opt-out",
        "reasons": [],
        "activity_identifier": "N/A",
        "user_id": random_uuid,
        "associated_value": "Marketing Preferences unsubscribed",
        "retailer": "test-retailer",
        "campaigns": [],
        "data": {
            "field_name": "test",
            "original_value": "test",
            "new_value": "testing",
        },
    }
