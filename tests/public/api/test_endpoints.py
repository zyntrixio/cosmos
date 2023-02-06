import uuid

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Callable

from pytest_mock import MockerFixture
from starlette import status

from cosmos.accounts.activity.enums import ActivityType as AccountActivityType
from cosmos.core.config import settings
from cosmos.core.error_codes import ErrorCode
from cosmos.db.models import AccountHolder, MarketingPreference, MarketingPreferenceValueTypes, Retailer, Reward
from cosmos.public.api.service import RESPONSE_TEMPLATE
from tests.conftest import SetupType

if TYPE_CHECKING:
    from fastapi.testclient import TestClient


PUBLIC_API_PREFIX = f"{settings.API_PREFIX}/public"


def test_opt_out_marketing_preferences(mocker: MockerFixture, setup: SetupType, test_client: "TestClient") -> None:
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
        activity_datetime=mp_true.updated_at.replace(tzinfo=timezone.utc),
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
    mocker: MockerFixture, setup: SetupType, test_client: "TestClient"
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
    mocker: MockerFixture, setup: SetupType, test_client: "TestClient"
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
    mocker: MockerFixture, setup: SetupType, test_client: "TestClient"
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
    mocker: MockerFixture, setup: SetupType, test_client: "TestClient"
) -> None:
    mock_sync_send_activity = mocker.patch("cosmos.public.api.service.async_send_activity")
    retailer = setup.retailer
    resp = test_client.get(
        f"{PUBLIC_API_PREFIX}/{retailer.slug}/marketing/unsubscribe",
    )

    assert resp.status_code == status.HTTP_202_ACCEPTED
    assert resp.text == RESPONSE_TEMPLATE.format(msg="You have opted out of any further marketing")
    mock_sync_send_activity.assert_not_called()


def test_get_reward_for_microsite(setup: SetupType, user_reward: Reward, test_client: "TestClient") -> None:
    db_session, retailer, account_holder = setup
    now = datetime.now(tz=timezone.utc)
    user_reward.expiry_date = now + timedelta(days=10)
    user_reward.account_holder = account_holder
    db_session.commit()

    resp = test_client.get(
        f"{PUBLIC_API_PREFIX}/{retailer.slug}/reward/{user_reward.reward_uuid}",
    )

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == {
        "code": f"{user_reward.code}",
        "expiry_date": f"{user_reward.expiry_date.date()}",
        "template_slug": f"{user_reward.reward_config.slug}",
        "status": "issued",
    }


def test_get_reward_for_microsite_invalid_reward_uuid(
    setup: SetupType, user_reward: Reward, test_client: "TestClient"
) -> None:
    db_session, retailer, account_holder = setup
    now = datetime.now(tz=timezone.utc)
    user_reward.expiry_date = now + timedelta(days=10)
    user_reward.account_holder = account_holder
    db_session.commit()

    resp = test_client.get(
        f"{PUBLIC_API_PREFIX}/{retailer.slug}/reward/wrong_uuid",
    )

    assert resp.status_code == ErrorCode.INVALID_REQUEST.value.status_code


def test_get_reward_for_microsite_invalid_retailer(
    setup: SetupType, user_reward: Reward, test_client: "TestClient"
) -> None:
    db_session, _, account_holder = setup
    now = datetime.now(tz=timezone.utc)
    user_reward.expiry_date = now + timedelta(days=10)
    user_reward.account_holder = account_holder
    db_session.commit()

    resp = test_client.get(
        f"{PUBLIC_API_PREFIX}/invalid-retailer/reward/{user_reward.reward_uuid}",
    )

    assert resp.status_code == ErrorCode.INVALID_REQUEST.value.status_code


def test_get_reward_for_microsite_past_expiry_date(
    setup: SetupType, user_reward: Reward, test_client: "TestClient"
) -> None:
    db_session, retailer, account_holder = setup
    now = datetime.now(tz=timezone.utc)
    user_reward.expiry_date = now + timedelta(days=-1)
    user_reward.account_holder = account_holder
    db_session.commit()

    resp = test_client.get(
        f"{PUBLIC_API_PREFIX}/{retailer.slug}/reward/{user_reward.reward_uuid}",
    )

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == {
        "code": f"{user_reward.code}",
        "expiry_date": f"{user_reward.expiry_date.date()}",
        "template_slug": f"{user_reward.reward_config.slug}",
        "status": "expired",
    }


def test_get_reward_for_microsite_redeemed_reward(
    setup: SetupType, user_reward: Reward, test_client: "TestClient"
) -> None:
    db_session, retailer, account_holder = setup
    now = datetime.now(tz=timezone.utc)
    user_reward.account_holder = account_holder
    user_reward.expiry_date = now + timedelta(days=10)
    user_reward.redeemed_date = now + timedelta(days=-5)
    db_session.commit()

    resp = test_client.get(
        f"{PUBLIC_API_PREFIX}/{retailer.slug}/reward/{user_reward.reward_uuid}",
    )

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == {
        "code": f"{user_reward.code}",
        "expiry_date": f"{user_reward.expiry_date.date()}",
        "template_slug": f"{user_reward.reward_config.slug}",
        "status": "redeemed",
        "redeemed_date": f"{user_reward.redeemed_date.date()}",
    }


def test_get_reward_for_microsite_bad_reward_uuid(
    setup: SetupType, user_reward: Reward, test_client: "TestClient"
) -> None:
    db_session, retailer, _ = setup
    now = datetime.now(tz=timezone.utc)
    user_reward.expiry_date = now + timedelta(days=10)
    db_session.commit()
    bad_reward_uuid = uuid.uuid4()

    resp = test_client.get(
        f"{PUBLIC_API_PREFIX}/{retailer.slug}/reward/{bad_reward_uuid}",
    )

    assert resp.status_code == ErrorCode.INVALID_REQUEST.value.status_code


def test_get_reward_for_microsite_reward_uuid_for_wrong_retailer(
    setup: SetupType,
    user_reward: Reward,
    test_client: "TestClient",
    create_mock_reward: Callable,
    create_mock_account_holder: Callable,
) -> None:
    db_session, retailer, _ = setup
    now = datetime.now(tz=timezone.utc)
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
