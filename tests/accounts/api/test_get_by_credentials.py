import uuid

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from fastapi import status as fastapi_http_status
from pytest_mock import MockerFixture

from cosmos.accounts.activity.enums import ActivityType as AccountsActivityType
from cosmos.accounts.config import account_settings
from cosmos.accounts.enums import AccountHolderStatuses
from cosmos.campaigns.enums import LoyaltyTypes
from cosmos.core.utils import generate_account_number
from cosmos.db.models import Campaign, CampaignBalance
from cosmos.retailers.enums import RetailerStatuses
from tests.accounts import accounts_auth_headers, client, validate_error_response
from tests.accounts.fixtures import errors
from tests.conftest import SetupType

if TYPE_CHECKING:
    from unittest.mock import MagicMock


def test_account_holder_get_by_credentials(
    setup: SetupType,
    mocker: MockerFixture,
    mock_campaign_balance_data: dict,
    campaign_with_rules: Campaign,
    create_retailer_store: Callable,
    create_mock_reward: Callable,
    create_pending_reward: Callable,
    create_transaction: Callable,
    create_transaction_earn: Callable,
    account_holder_campaign_balances: list[CampaignBalance],
    mock_activity: "MagicMock",
) -> None:

    db_session, retailer, account_holder = setup
    account_holder.status = AccountHolderStatuses.ACTIVE
    account_holder.account_number = "TEST123456789"
    db_session.commit()

    now = datetime.now(tz=UTC)
    mock_datetime = mocker.patch("cosmos.accounts.api.service.datetime")
    mock_datetime.now.return_value = now
    create_mock_reward(
        **{
            "account_holder_id": account_holder.id,
            "code": "code1",
            "reward_uuid": str(uuid.uuid4()),
            "issued_date": now,
            "expiry_date": now + timedelta(days=10),  # expired
            "retailer_id": retailer.id,
            "campaign_id": campaign_with_rules.id,
        }
    )
    create_mock_reward(
        **{
            "account_holder_id": account_holder.id,
            "code": "code2",
            "reward_uuid": str(uuid.uuid4()),
            "issued_date": now,
            "expiry_date": now - timedelta(days=10),  # expired
            "retailer_id": retailer.id,
            "campaign_id": campaign_with_rules.id,
        }
    )
    create_pending_reward(
        **{
            "account_holder_id": account_holder.id,
            "created_date": now,
            "conversion_date": now + timedelta(days=10),
            "campaign_id": campaign_with_rules.id,
            "count": 1,
        }
    )
    fake_mid = "mid-potato"
    store = create_retailer_store(retailer_id=retailer.id, mid=fake_mid, store_name="potatoesRus")
    transactions = []

    total_num_transactions = 15
    for i in range(1, total_num_transactions + 1):
        amount = i * 1000
        dt = datetime(2023, 1, i, tzinfo=UTC)
        tx = create_transaction(
            account_holder=account_holder,
            **{"mid": fake_mid, "transaction_id": f"tx-id-{i}", "amount": amount, "datetime": dt},
        )
        transactions.append(tx)
        create_transaction_earn(
            tx,
            earn_amount=amount,
            loyalty_type=LoyaltyTypes.ACCUMULATOR,
        )

    resp = client.post(
        f"{account_settings.ACCOUNT_API_PREFIX}/{retailer.slug}/accounts/getbycredentials",
        json={"email": account_holder.email, "account_number": account_holder.account_number},
        headers=accounts_auth_headers,
    )

    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["UUID"] == str(account_holder.account_holder_uuid)

    # current_balances are transformed into a list for the JSON response
    assert resp_json["current_balances"] == mock_campaign_balance_data

    assert len(resp_json["rewards"]) == 2
    assert len(resp_json["pending_rewards"]) == 1
    assert len(resp_json["transaction_history"]) == 10

    expected_transaction_history = []
    for i in reversed(range(total_num_transactions - 10 + 1, total_num_transactions + 1)):
        amount_and_earned = f"{i*1000/100:.2f}"
        expected_transaction_history.append(
            {
                "datetime": int(datetime(2023, 1, i, tzinfo=UTC).replace(tzinfo=None).timestamp()),
                "amount": amount_and_earned,
                "amount_currency": "GBP",
                "location": store.store_name,
                "loyalty_earned_value": amount_and_earned,
                "loyalty_earned_type": "ACCUMULATOR",
            }
        )

    assert resp_json["transaction_history"] == expected_transaction_history
    rewards = sorted(resp_json["rewards"], key=lambda x: x["code"])
    assert rewards[0]["code"] == "code1"
    assert rewards[0]["status"] == "issued"
    assert rewards[0]["redeemed_date"] is None

    assert rewards[1]["code"] == "code2"
    assert rewards[1]["status"] == "expired"
    assert rewards[1]["redeemed_date"] is None

    assert "created_date" in resp_json["pending_rewards"][0]
    assert "conversion_date" in resp_json["pending_rewards"][0]
    assert "campaign_slug" in resp_json["pending_rewards"][0]

    expected_payload = {
        "account_holder_uuid": str(account_holder.account_holder_uuid),
        "activity_datetime": now,
        "retailer_slug": retailer.slug,
        "channel": "channel",
    }
    mock_activity.assert_called_once_with(
        activity_type=AccountsActivityType.ACCOUNT_AUTHENTICATION,
        payload_formatter_fn=AccountsActivityType.get_account_auth_activity_data,
        formatter_kwargs=expected_payload,
    )


def test_account_holder_get_by_credentials_no_balances(setup: SetupType, mock_activity: "MagicMock") -> None:
    db_session, retailer, account_holder = setup
    account_holder.status = AccountHolderStatuses.ACTIVE
    account_holder.account_number = "TEST123456789"
    db_session.execute(
        CampaignBalance.__table__.delete().where(  #  type: ignore [ attr-defined]
            CampaignBalance.account_holder_id == account_holder.id
        )
    )
    db_session.commit()

    for retailer_status, expected_balances in (
        (RetailerStatuses.TEST, [{"campaign_slug": "N/A", "value": 0}]),
        (RetailerStatuses.ACTIVE, []),
    ):

        retailer.status = retailer_status
        db_session.commit()

        resp = client.post(
            f"{account_settings.ACCOUNT_API_PREFIX}/{retailer.slug}/accounts/getbycredentials",
            json={"email": account_holder.email, "account_number": account_holder.account_number},
            headers=accounts_auth_headers,
        )

        assert resp.status_code == 200
        resp_json = resp.json()
        assert resp_json["UUID"] == str(account_holder.account_holder_uuid)
        assert resp_json["current_balances"] == expected_balances
        assert resp_json["rewards"] == []
        assert resp_json["pending_rewards"] == []
        assert resp_json["transaction_history"] == []
        mock_activity.assert_called()


def test_account_holder_get_by_credentials_inactive_retailer(setup: SetupType, mock_activity: "MagicMock") -> None:
    db_session, retailer, account_holder = setup

    if account_holder.account_number is None:
        account_holder.account_number = generate_account_number(retailer.account_number_prefix)
        db_session.commit()

    retailer.status = RetailerStatuses.INACTIVE
    db_session.commit()

    resp = client.post(
        f"{account_settings.ACCOUNT_API_PREFIX}/{retailer.slug}/accounts/getbycredentials",
        json={"email": account_holder.email, "account_number": account_holder.account_number},
        headers=accounts_auth_headers,
    )

    validate_error_response(resp, errors.INACTIVE_RETAILER)
    mock_activity.assert_not_called()


def test_account_holder_get_by_credentials_inactive_account(setup: SetupType, mock_activity: "MagicMock") -> None:
    db_session, retailer, account_holder = setup

    if account_holder.account_number is None:
        account_holder.account_number = generate_account_number(retailer.account_number_prefix)
        db_session.commit()

    account_holder.status = AccountHolderStatuses.INACTIVE
    db_session.commit()

    resp = client.post(
        f"{account_settings.ACCOUNT_API_PREFIX}/{retailer.slug}/accounts/getbycredentials",
        json={"email": account_holder.email, "account_number": account_holder.account_number},
        headers=accounts_auth_headers,
    )

    validate_error_response(resp, errors.NO_ACCOUNT_FOUND)
    mock_activity.assert_not_called()


def test_account_holder_get_by_credentials_invalid_retailer(mock_activity: "MagicMock") -> None:
    resp = client.post(
        f"{account_settings.ACCOUNT_API_PREFIX}/INVALID/accounts/getbycredentials",
        json={"email": "test@mail.com", "account_number": "will fail before this"},
        headers=accounts_auth_headers,
    )

    validate_error_response(resp, errors.INVALID_RETAILER)
    mock_activity.assert_not_called()


def test_account_holder_get_by_credentials_missing_user(setup: SetupType, mock_activity: "MagicMock") -> None:
    retailer = setup.retailer

    resp = client.post(
        f"{account_settings.ACCOUNT_API_PREFIX}/{retailer.slug}/accounts/getbycredentials",
        json={"email": "test@mail.com", "account_number": "DOESNOTEXISTS"},
        headers=accounts_auth_headers,
    )

    validate_error_response(resp, errors.NO_ACCOUNT_FOUND)
    mock_activity.assert_not_called()


def test_account_holder_get_by_credentials_mangled_json(setup: SetupType) -> None:
    retailer = setup.retailer

    resp = client.post(
        f"{account_settings.ACCOUNT_API_PREFIX}/{retailer.slug}/accounts/getbycredentials",
        data=b"{",  # type: ignore [arg-type]
        headers=accounts_auth_headers,
    )

    assert resp.status_code == fastapi_http_status.HTTP_400_BAD_REQUEST
    assert resp.json() == {
        "display_message": "Malformed request.",
        "code": "MALFORMED_REQUEST",
    }


def test_account_holder_get_by_credentials_invalid_token(setup: SetupType, mock_activity: "MagicMock") -> None:
    _, retailer, account_holder = setup

    resp = client.post(
        f"{account_settings.ACCOUNT_API_PREFIX}/{retailer.slug}/accounts/getbycredentials",
        json={"email": account_holder.email, "account_number": account_holder.account_number},
        headers={"Authorization": "Token wrong token e.g. potato"},
    )

    assert resp.status_code == fastapi_http_status.HTTP_401_UNAUTHORIZED
    assert resp.json() == {
        "display_message": "Supplied token is invalid.",
        "code": "INVALID_TOKEN",
    }
    mock_activity.assert_not_called()
