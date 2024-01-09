import uuid

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from deepdiff import DeepDiff
from fastapi import status
from pytest_mock import MockerFixture

from cosmos.accounts.activity.enums import ActivityType as AccountActivityType
from cosmos.core.error_codes import ErrorCode
from cosmos.db.models import (
    AccountHolder,
    AccountHolderEmail,
    MarketingPreference,
    MarketingPreferenceValueTypes,
    Retailer,
    Reward,
)
from cosmos.public.api.service import RESPONSE_TEMPLATE, CallbackService
from cosmos.public.config import public_settings

if TYPE_CHECKING:
    from fastapi.testclient import TestClient

    from cosmos.db.models import EmailType
    from tests.conftest import SetupType

PUBLIC_API_PREFIX = public_settings.PUBLIC_API_PREFIX


def test_opt_out_marketing_preferences(mocker: MockerFixture, setup: "SetupType", test_client: "TestClient") -> None:
    db_session, retailer, account_holder = setup
    mock_get_marketing_preference_change_activity_data = mocker.patch(
        "cosmos.accounts.activity.enums.ActivityType.get_marketing_preference_change_activity_data",
        return_value={"mock": "payload"},
    )
    mock_sync_send_activity = mocker.patch("cosmos.public.api.service.async_send_activity")
    mp_true = MarketingPreference(
        account_holder_id=account_holder.id,
        key_name="preference-one",
        value="True",
        value_type=MarketingPreferenceValueTypes.BOOLEAN,
    )
    db_session.add(mp_true)

    mp_false = MarketingPreference(
        account_holder_id=account_holder.id,
        key_name="preference-two",
        value="False",
        value_type=MarketingPreferenceValueTypes.BOOLEAN,
    )
    db_session.add(mp_false)

    mp_not_boolean = MarketingPreference(
        account_holder_id=account_holder.id,
        key_name="preference-three",
        value="potato",
        value_type=MarketingPreferenceValueTypes.STRING,
    )
    db_session.add(mp_not_boolean)
    db_session.commit()

    opt_out_token = account_holder.opt_out_token
    resp = test_client.get(
        f"{PUBLIC_API_PREFIX}/{retailer.slug}/marketing/unsubscribe?u={opt_out_token}",
    )
    db_session.refresh(mp_true)
    db_session.refresh(mp_false)
    db_session.refresh(mp_not_boolean)

    assert resp.status_code == status.HTTP_202_ACCEPTED
    assert resp.text == RESPONSE_TEMPLATE.format(msg=f"You have opted out of any further marketing for {retailer.name}")
    assert mp_true.value == "False"
    assert mp_false.value == "False"
    assert mp_not_boolean.value == "potato"
    mock_get_marketing_preference_change_activity_data.assert_called_once_with(
        account_holder_uuid=account_holder.account_holder_uuid,
        activity_datetime=mp_true.updated_at.replace(tzinfo=UTC),
        associated_value="Marketing Preferences unsubscribed",
        field_name="preference-one",
        new_value="False",
        original_value="True",
        retailer_slug="re-test",
        summary="Unsubscribed via marketing opt-out",
    )
    mock_sync_send_activity.assert_called_once_with(
        {"mock": "payload"}, routing_key=AccountActivityType.ACCOUNT_CHANGE.value
    )


def test_opt_out_marketing_preferences_wrong_retailer(
    mocker: MockerFixture, setup: "SetupType", test_client: "TestClient"
) -> None:
    db_session = setup.db_session
    account_holder = setup.account_holder
    mock_sync_send_activity = mocker.patch("cosmos.public.api.service.async_send_activity")
    mp_true = MarketingPreference(
        account_holder_id=account_holder.id,
        key_name="preference-one",
        value="True",
        value_type=MarketingPreferenceValueTypes.BOOLEAN,
    )
    db_session.add(mp_true)
    db_session.commit()
    opt_out_token = account_holder.opt_out_token
    resp = test_client.get(
        f"{PUBLIC_API_PREFIX}/WRONG-RETAILER/marketing/unsubscribe?u={opt_out_token}",
    )
    db_session.refresh(mp_true)

    assert resp.status_code == status.HTTP_202_ACCEPTED
    assert resp.text == RESPONSE_TEMPLATE.format(msg="You have opted out of any further marketing")
    assert mp_true.value == "True"
    mock_sync_send_activity.assert_not_called()


def test_opt_out_marketing_preferences_invalid_opt_out_token(
    mocker: MockerFixture, setup: "SetupType", test_client: "TestClient"
) -> None:
    mock_sync_send_activity = mocker.patch("cosmos.public.api.service.async_send_activity")
    retailer = setup.retailer
    resp = test_client.get(
        f"{PUBLIC_API_PREFIX}/{retailer.slug}/marketing/unsubscribe?u=WRONG-TOKEN",
    )

    assert resp.status_code == status.HTTP_202_ACCEPTED
    assert resp.text == RESPONSE_TEMPLATE.format(msg="You have opted out of any further marketing")
    mock_sync_send_activity.assert_not_called()


def test_opt_out_marketing_preferences_wrong_opt_out_token(
    mocker: MockerFixture, setup: "SetupType", test_client: "TestClient"
) -> None:
    mock_sync_send_activity = mocker.patch("cosmos.public.api.service.async_send_activity")
    retailer = setup.retailer
    resp = test_client.get(
        f"{PUBLIC_API_PREFIX}/{retailer.slug}/marketing/unsubscribe?u={uuid.uuid4()}",
    )

    assert resp.status_code == status.HTTP_202_ACCEPTED
    assert resp.text == RESPONSE_TEMPLATE.format(msg="You have opted out of any further marketing")
    mock_sync_send_activity.assert_not_called()


def test_opt_out_marketing_preferences_no_opt_out_token_provided(
    mocker: MockerFixture, setup: "SetupType", test_client: "TestClient"
) -> None:
    mock_sync_send_activity = mocker.patch("cosmos.public.api.service.async_send_activity")
    retailer = setup.retailer
    resp = test_client.get(
        f"{PUBLIC_API_PREFIX}/{retailer.slug}/marketing/unsubscribe",
    )

    assert resp.status_code == status.HTTP_202_ACCEPTED
    assert resp.text == RESPONSE_TEMPLATE.format(msg="You have opted out of any further marketing")
    mock_sync_send_activity.assert_not_called()


def test_get_reward_for_microsite(setup: "SetupType", user_reward: Reward, test_client: "TestClient") -> None:
    db_session, retailer, account_holder = setup
    now = datetime.now(tz=UTC)
    user_reward.expiry_date = now + timedelta(days=10)
    user_reward.account_holder = account_holder
    db_session.commit()

    resp = test_client.get(
        f"{PUBLIC_API_PREFIX}/{retailer.slug}/reward/{user_reward.reward_uuid}",
    )

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == {
        "code": str(user_reward.code),
        "expiry_date": str(user_reward.expiry_date.date()),
        "template_slug": str(user_reward.reward_config.slug),
        "status": "issued",
    }


def test_get_reward_for_microsite_invalid_reward_uuid(
    setup: "SetupType", user_reward: Reward, test_client: "TestClient"
) -> None:
    db_session, retailer, account_holder = setup
    now = datetime.now(tz=UTC)
    user_reward.expiry_date = now + timedelta(days=10)
    user_reward.account_holder = account_holder
    db_session.commit()

    resp = test_client.get(
        f"{PUBLIC_API_PREFIX}/{retailer.slug}/reward/wrong_uuid",
    )

    assert resp.status_code == ErrorCode.INVALID_REQUEST.value.status_code


def test_get_reward_for_microsite_invalid_retailer(
    setup: "SetupType", user_reward: Reward, test_client: "TestClient"
) -> None:
    db_session, _, account_holder = setup
    now = datetime.now(tz=UTC)
    user_reward.expiry_date = now + timedelta(days=10)
    user_reward.account_holder = account_holder
    db_session.commit()

    resp = test_client.get(
        f"{PUBLIC_API_PREFIX}/invalid-retailer/reward/{user_reward.reward_uuid}",
    )

    assert resp.status_code == ErrorCode.INVALID_REQUEST.value.status_code


def test_get_reward_for_microsite_past_expiry_date(
    setup: "SetupType", user_reward: Reward, test_client: "TestClient"
) -> None:
    db_session, retailer, account_holder = setup
    now = datetime.now(tz=UTC)
    user_reward.expiry_date = now + timedelta(days=-1)
    user_reward.account_holder = account_holder
    db_session.commit()

    resp = test_client.get(
        f"{PUBLIC_API_PREFIX}/{retailer.slug}/reward/{user_reward.reward_uuid}",
    )

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == {
        "code": str(user_reward.code),
        "expiry_date": str(user_reward.expiry_date.date()),
        "template_slug": str(user_reward.reward_config.slug),
        "status": "expired",
    }


def test_get_reward_for_microsite_redeemed_reward(
    setup: "SetupType", user_reward: Reward, test_client: "TestClient"
) -> None:
    db_session, retailer, account_holder = setup
    now = datetime.now(tz=UTC)
    user_reward.account_holder = account_holder
    user_reward.expiry_date = now + timedelta(days=10)
    user_reward.redeemed_date = now + timedelta(days=-5)
    db_session.commit()

    resp = test_client.get(
        f"{PUBLIC_API_PREFIX}/{retailer.slug}/reward/{user_reward.reward_uuid}",
    )

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == {
        "code": str(user_reward.code),
        "expiry_date": str(user_reward.expiry_date.date()),
        "template_slug": str(user_reward.reward_config.slug),
        "status": "redeemed",
        "redeemed_date": str(user_reward.redeemed_date.date()),
    }


def test_get_reward_for_microsite_bad_reward_uuid(
    setup: "SetupType", user_reward: Reward, test_client: "TestClient"
) -> None:
    db_session, retailer, _ = setup
    now = datetime.now(tz=UTC)
    user_reward.expiry_date = now + timedelta(days=10)
    db_session.commit()
    bad_reward_uuid = uuid.uuid4()

    resp = test_client.get(
        f"{PUBLIC_API_PREFIX}/{retailer.slug}/reward/{bad_reward_uuid}",
    )

    assert resp.status_code == ErrorCode.INVALID_REQUEST.value.status_code


def test_get_reward_for_microsite_reward_uuid_for_wrong_retailer(
    setup: "SetupType",
    user_reward: Reward,
    test_client: "TestClient",
    create_mock_reward: Callable,
    create_mock_account_holder: Callable,
) -> None:
    db_session, retailer, _ = setup
    now = datetime.now(tz=UTC)
    user_reward.expiry_date = now + timedelta(days=10)
    random_uuid = uuid.uuid4()
    wrong_retailer = Retailer(
        id=100,
        name="test",
        slug="wrong-retailer",
        status="TEST",
        account_number_prefix="LYTEST",
        account_number_length=10,
        profile_config="potato-fest-21",
        marketing_preference_config="random",
        loyalty_name="wrong-retailer",
    )
    db_session.add(wrong_retailer)

    account_holder_2: AccountHolder = create_mock_account_holder(
        retailer_id=wrong_retailer.id, **{"email": "activate_2@test.user"}
    )
    create_mock_reward(
        **{
            "account_holder": account_holder_2,
            "reward_uuid": random_uuid,
            "retailer": wrong_retailer,
        }
    )

    db_session.commit()

    resp = test_client.get(
        f"{PUBLIC_API_PREFIX}/{retailer.slug}/reward/{random_uuid}",
    )

    assert resp.status_code == ErrorCode.INVALID_REQUEST.value.status_code


def test_account_holder_email_callback_event_wrong_auth(test_client: "TestClient", mocker: MockerFixture) -> None:
    mocked_format_and_send_activity = mocker.patch.object(CallbackService, "format_and_send_stored_activities")

    resp = test_client.post(f"{public_settings.PUBLIC_API_PREFIX}/email/event")

    assert resp.status_code == status.HTTP_401_UNAUTHORIZED
    mocked_format_and_send_activity.assert_not_called()

    resp = test_client.post(f"{public_settings.PUBLIC_API_PREFIX}/email/event", auth=("ping", "pong"))

    assert resp.status_code == status.HTTP_401_UNAUTHORIZED
    mocked_format_and_send_activity.assert_not_called()


def test_account_holder_email_callback_missing_guid(test_client: "TestClient", mocker: MockerFixture) -> None:
    mocked_format_and_send_activity = mocker.patch.object(CallbackService, "format_and_send_stored_activities")
    mock_logger = mocker.patch("cosmos.public.api.endpoints.logger")

    payload = {
        "event": "test",
        "time": 1433333949,
        "MessageID": 19421777835146490,
        "email": "api@mailjet.com",
        "mj_campaign_id": 7257,
        "mj_contact_id": 4,
        "customcampaign": "",
        "mj_message_id": "19421777835146490",
        "smtp_reply": "sent (250 2.0.0 OK 1433333948 fa5si855896wjc.199 - gsmtp)",
        "CustomID": "helloworld",
    }
    resp = test_client.post(
        f"{public_settings.PUBLIC_API_PREFIX}/email/event",
        auth=(
            public_settings.MAIL_EVENT_CALLBACK_USERNAME,
            public_settings.MAIL_EVENT_CALLBACK_PASSWORD,
        ),
        json=payload,
    )

    assert resp.status_code == status.HTTP_200_OK

    mocked_format_and_send_activity.assert_not_called()
    mock_logger.exception.assert_called_once_with("failed to parse payload %s", payload)


def test_account_holder_email_callback_event_message_uuid_not_found(
    test_client: "TestClient", mocker: MockerFixture
) -> None:
    mocked_format_and_send_activity = mocker.patch.object(CallbackService, "format_and_send_stored_activities")
    message_uuid = uuid4()
    new_status = "open"

    mock_logger = mocker.MagicMock()
    mocker.patch("cosmos.core.api.service.logging", getLogger=lambda _: mock_logger)

    resp = test_client.post(
        f"{public_settings.PUBLIC_API_PREFIX}/email/event",
        auth=(
            public_settings.MAIL_EVENT_CALLBACK_USERNAME,
            public_settings.MAIL_EVENT_CALLBACK_PASSWORD,
        ),
        json={
            "event": new_status,
            "time": 1433333949,
            "MessageID": 19421777835146490,
            "Message_GUID": str(message_uuid),
            "email": "api@mailjet.com",
            "mj_campaign_id": 7257,
            "mj_contact_id": 4,
            "customcampaign": "",
            "mj_message_id": "19421777835146490",
            "smtp_reply": "sent (250 2.0.0 OK 1433333948 fa5si855896wjc.199 - gsmtp)",
            "CustomID": "helloworld",
        },
    )
    assert resp.status_code == status.HTTP_200_OK
    mock_logger.exception.assert_called_once_with(
        "Failed to update AccountHolderEmail with message_uuid %s with current_status %s",
        message_uuid,
        new_status,
    )
    mocked_format_and_send_activity.assert_not_called()


@pytest.mark.parametrize(
    "payload_is_list",
    (
        pytest.param(True, id="Payload is a list of dicts"),
        pytest.param(False, id="Payload is a dict"),
    ),
)
def test_account_holder_email_callback_event_ok(
    payload_is_list: bool,
    test_client: "TestClient",
    setup: "SetupType",
    balance_reset_email_type: "EmailType",
    mocker: MockerFixture,
) -> None:
    db_session, retailer, account_holder = setup
    mock_now = datetime.now(tz=UTC)

    mock_datetime = mocker.patch("cosmos.core.activity.enums.datetime")
    mock_datetime.now.return_value = mock_now

    sent_activities = []

    async def format_stored_activities(self: CallbackService) -> None:
        nonlocal sent_activities

        for stored_activity in self._stored_activities:
            sent_activities.append(stored_activity["payload_formatter_fn"](**stored_activity["formatter_kwargs"]))

    mocker.patch.object(CallbackService, "format_and_send_stored_activities", format_stored_activities)

    event_timestamp = int(mock_now.timestamp())
    event_datetime = datetime.fromtimestamp(event_timestamp, tz=UTC)
    message_uuid = uuid4()
    new_status = "open"
    account_holder_email = AccountHolderEmail(
        account_holder_id=account_holder.id,
        email_type_id=balance_reset_email_type.id,
        message_uuid=message_uuid,
    )
    db_session.add(account_holder_email)
    db_session.commit()

    assert not account_holder_email.current_status

    payload = {
        "event": new_status,
        "time": event_timestamp,
        "MessageID": 19421777835146490,
        "Message_GUID": str(message_uuid),
        "email": "api@mailjet.com",
        "mj_campaign_id": 7257,
        "mj_contact_id": 4,
        "customcampaign": "",
        "mj_message_id": "19421777835146490",
        "smtp_reply": "sent (250 2.0.0 OK 1433333948 fa5si855896wjc.199 - gsmtp)",
        "CustomID": "helloworld",
    }

    resp = test_client.post(
        f"{public_settings.PUBLIC_API_PREFIX}/email/event",
        auth=(
            public_settings.MAIL_EVENT_CALLBACK_USERNAME,
            public_settings.MAIL_EVENT_CALLBACK_PASSWORD,
        ),
        json=[payload] if payload_is_list else payload,
    )

    assert resp.status_code == status.HTTP_200_OK

    db_session.refresh(account_holder_email)
    assert account_holder_email.current_status == new_status
    assert len(sent_activities) == 1
    assert not DeepDiff(
        sent_activities[0],
        {
            "type": "EMAIL_EVENT",
            "datetime": mock_now,
            "underlying_datetime": event_datetime,
            "summary": f"{new_status} Mailjet event received",
            "reasons": ["MailJet Event received"],
            "activity_identifier": str(message_uuid),
            "user_id": str(account_holder.account_holder_uuid),
            "associated_value": new_status,
            "retailer": retailer.slug,
            "campaigns": [],
            "data": {
                "event": new_status,
                "time": event_timestamp,
                "MessageID": 19421777835146490,
                "Message_GUID": str(message_uuid),
                "email": "api@mailjet.com",
                "mj_campaign_id": 7257,
                "mj_contact_id": 4,
                "customcampaign": "",
                "mj_message_id": "19421777835146490",
                "smtp_reply": "sent (250 2.0.0 OK 1433333948 fa5si855896wjc.199 - gsmtp)",
                "CustomID": "helloworld",
            },
        },
    )
