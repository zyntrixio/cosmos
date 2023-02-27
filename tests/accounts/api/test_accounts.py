import uuid

from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from pytest_mock import MockerFixture

from cosmos.accounts.activity.enums import ActivityType as AccountsActivityType
from cosmos.accounts.config import account_settings
from cosmos.accounts.enums import AccountHolderStatuses
from cosmos.campaigns.enums import LoyaltyTypes
from cosmos.db.models import Campaign, CampaignBalance
from cosmos.retailers.enums import RetailerStatuses
from tests.accounts import accounts_auth_headers, client, validate_error_response
from tests.accounts.fixtures import errors
from tests.conftest import SetupType


def test_account_holder_get_by_id(
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
    db_session.commit()

    now = datetime.now(tz=timezone.utc)
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

    total_num_transactions = 10
    for i in range(1, total_num_transactions + 1):
        amount = i * 1000
        dt = datetime(2023, 1, i, tzinfo=timezone.utc)
        tx = create_transaction(
            account_holder=account_holder,
            **{"mid": fake_mid, "transaction_id": f"tx-id-{i}", "amount": amount, "datetime": dt},
        )
        transactions.append(tx)
        create_transaction_earn(
            tx,
            earn_amount=amount,
            loyalty_type=LoyaltyTypes.ACCUMULATOR,
            earn_rule=campaign_with_rules.earn_rule,
        )

    num_tx_histories = 5
    resp = client.get(
        f"{account_settings.ACCOUNT_API_PREFIX}/{retailer.slug}/accounts/{account_holder.account_holder_uuid}"
        f"?tx_qty={num_tx_histories}",
        headers=accounts_auth_headers,
    )

    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["UUID"] == str(account_holder.account_holder_uuid)

    # current_balances are transformed into a list for the JSON response
    assert resp_json["current_balances"] == mock_campaign_balance_data

    assert len(resp_json["rewards"]) == 2
    assert len(resp_json["pending_rewards"]) == 1
    assert len(resp_json["transaction_history"]) == 5

    expected_transaction_history = []
    for i in reversed(range(num_tx_histories + 1, total_num_transactions + 1)):
        amount_and_earned = f"{i*1000/100:.2f}"
        expected_transaction_history.append(
            {
                "datetime": int(datetime(2023, 1, i, tzinfo=timezone.utc).replace(tzinfo=None).timestamp()),
                "amount": amount_and_earned,
                "amount_currency": "GBP",
                "location": store.store_name,
                "loyalty_earned_value": amount_and_earned,
                "loyalty_earned_type": "ACCUMULATOR",
            }
        )

    assert resp_json["transaction_history"] == expected_transaction_history
    assert resp_json["rewards"][0]["code"] == "code1"
    assert resp_json["rewards"][0]["status"] == "issued"
    assert resp_json["rewards"][0]["redeemed_date"] is None

    assert resp_json["rewards"][1]["code"] == "code2"
    assert resp_json["rewards"][1]["status"] == "expired"
    assert resp_json["rewards"][1]["redeemed_date"] is None

    assert "created_date" in resp_json["pending_rewards"][0]
    assert "conversion_date" in resp_json["pending_rewards"][0]
    assert "campaign_slug" in resp_json["pending_rewards"][0]

    expected_payload = {
        "account_holder_uuid": str(account_holder.account_holder_uuid),
        "activity_datetime": now,
        "retailer_slug": retailer.slug,
        "channel": "channel",
        "campaign_slugs": {"test-campaign"},
    }
    mock_activity.assert_called_once_with(
        activity_type=AccountsActivityType.ACCOUNT_VIEW,
        payload_formatter_fn=AccountsActivityType.get_account_activity_data,
        formatter_kwargs=expected_payload,
    )


def test_account_holder_get_by_id_stamps_tx_history(
    setup: SetupType,
    mocker: MockerFixture,
    campaign_with_rules: Campaign,
    create_retailer_store: Callable,
    create_transaction: Callable,
    create_transaction_earn: Callable,
    mock_activity: "MagicMock",
) -> None:

    db_session, retailer, account_holder = setup
    account_holder.status = AccountHolderStatuses.ACTIVE
    db_session.commit()

    now = datetime.now(tz=timezone.utc)
    mock_datetime = mocker.patch("cosmos.accounts.api.service.datetime")
    mock_datetime.now.return_value = now
    fake_mid = "mid-potato"
    store = create_retailer_store(retailer_id=retailer.id, mid=fake_mid, store_name="potatoesRus")
    transactions = []

    total_num_transactions = 10
    for i in range(1, total_num_transactions + 1):
        amount = i * 1000
        dt = datetime(2023, 1, i, tzinfo=timezone.utc)
        tx = create_transaction(
            account_holder=account_holder,
            **{"mid": fake_mid, "transaction_id": f"tx-id-{i}", "amount": amount, "datetime": dt},
        )
        transactions.append(tx)
        create_transaction_earn(
            tx,
            earn_amount=amount,
            loyalty_type=LoyaltyTypes.STAMPS,
            earn_rule=campaign_with_rules.earn_rule,
        )

    num_tx_histories = 5
    resp = client.get(
        f"{account_settings.ACCOUNT_API_PREFIX}/{retailer.slug}/accounts/{account_holder.account_holder_uuid}"
        f"?tx_qty={num_tx_histories}",
        headers=accounts_auth_headers,
    )

    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["UUID"] == str(account_holder.account_holder_uuid)

    expected_transaction_history = []
    for i in reversed(range(num_tx_histories + 1, total_num_transactions + 1)):
        tx_amount = f"{i*1000/100:.2f}"
        earned = str(int(float(tx_amount)))
        expected_transaction_history.append(
            {
                "datetime": int(datetime(2023, 1, i, tzinfo=timezone.utc).replace(tzinfo=None).timestamp()),
                "amount": tx_amount,
                "amount_currency": "GBP",
                "location": store.store_name,
                "loyalty_earned_value": earned,
                "loyalty_earned_type": "STAMPS",
            }
        )

    assert resp_json["transaction_history"] == expected_transaction_history


def test_account_holder_get_by_id_no_balances(setup: SetupType, mock_activity: "MagicMock") -> None:
    db_session, retailer, account_holder = setup
    account_holder.status = AccountHolderStatuses.ACTIVE

    db_session.execute(CampaignBalance.__table__.delete().where(CampaignBalance.account_holder_id == account_holder.id))
    db_session.commit()

    for retailer_status, expected_balances in (
        (RetailerStatuses.TEST, [{"campaign_slug": "N/A", "value": 0}]),
        (RetailerStatuses.ACTIVE, []),
    ):

        retailer.status = retailer_status
        db_session.commit()

        resp = client.get(
            f"{account_settings.ACCOUNT_API_PREFIX}/{retailer.slug}/accounts/{account_holder.account_holder_uuid}?tx_qty=5",
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


def test_account_holder_get_by_id_pending_reward_count_more_than_one(
    setup: SetupType,
    campaign: Campaign,
    create_pending_reward: Callable,
) -> None:

    db_session, retailer, account_holder = setup
    account_holder.status = AccountHolderStatuses.ACTIVE
    db_session.commit()

    created_date = datetime.now(tz=timezone.utc).replace(tzinfo=None)
    conversion_date = created_date + timedelta(days=10)

    pending_reward_value = 100
    pending_reward_count = 3

    create_pending_reward(
        **{
            "account_holder_id": account_holder.id,
            "created_date": created_date,
            "conversion_date": conversion_date,
            "campaign_id": campaign.id,
            "value": pending_reward_value,
            "count": pending_reward_count,
            "total_cost_to_user": pending_reward_value * pending_reward_count,
        }
    )

    resp = client.get(
        f"{account_settings.ACCOUNT_API_PREFIX}/{retailer.slug}/accounts/{account_holder.account_holder_uuid}",
        headers=accounts_auth_headers,
    )

    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["UUID"] == str(account_holder.account_holder_uuid)

    assert len(resp_json["pending_rewards"]) == 3
    for pending_reward in resp_json["pending_rewards"]:
        assert pending_reward["created_date"] == int(created_date.timestamp())
        assert pending_reward["conversion_date"] == int(conversion_date.timestamp())
        assert pending_reward["campaign_slug"] == campaign.slug


def test_account_holder_get_by_id_inactive_account(setup: SetupType, mock_activity: "MagicMock") -> None:
    db_session, retailer, account_holder = setup

    account_holder.status = AccountHolderStatuses.INACTIVE
    db_session.commit()

    resp = client.get(
        f"{account_settings.ACCOUNT_API_PREFIX}/{retailer.slug}/accounts/{account_holder.account_holder_uuid}",
        headers=accounts_auth_headers,
    )

    validate_error_response(resp, errors.NO_ACCOUNT_FOUND)
    mock_activity.assert_not_called()


def test_account_holder_get_by_id_malformed_uuid(setup: SetupType, mock_activity: "MagicMock") -> None:
    retailer = setup.retailer

    resp = client.get(
        f"{account_settings.ACCOUNT_API_PREFIX}/{retailer.slug}/accounts/NOT-A-VALID-UUID",
        headers=accounts_auth_headers,
    )

    validate_error_response(resp, errors.NO_ACCOUNT_FOUND)
    mock_activity.assert_not_called()


def test_get_account_holder_get_by_id_invalid_token(setup: SetupType, mock_activity: "MagicMock") -> None:
    _, retailer, account_holder = setup

    resp = client.get(
        f"{account_settings.ACCOUNT_API_PREFIX}/{retailer.slug}/accounts/{account_holder.account_holder_uuid}",
        headers={"Authorization": "Token wrong token e.g. potato"},
    )

    validate_error_response(resp, errors.INVALID_TOKEN)
    mock_activity.assert_not_called()


def test_account_holder_get_by_id_tx_history_no_store(
    setup: SetupType,
    mocker: MockerFixture,
    campaign_with_rules: Campaign,
    create_transaction: Callable,
    create_transaction_earn: Callable,
    mock_activity: "MagicMock",
) -> None:

    db_session, retailer, account_holder = setup
    account_holder.status = AccountHolderStatuses.ACTIVE
    db_session.commit()

    now = datetime.now(tz=timezone.utc)
    mock_datetime = mocker.patch("cosmos.accounts.api.service.datetime")
    mock_datetime.now.return_value = now
    amount = 1000
    dt = datetime(2023, 1, 1, tzinfo=timezone.utc)
    tx = create_transaction(
        account_holder=account_holder,
        **{"mid": "a-mid", "transaction_id": "tx-id", "amount": amount, "datetime": dt},
    )
    create_transaction_earn(
        tx,
        earn_amount=amount,
        loyalty_type=LoyaltyTypes.ACCUMULATOR,
        earn_rule=campaign_with_rules.earn_rule,
    )

    resp = client.get(
        f"{account_settings.ACCOUNT_API_PREFIX}/{retailer.slug}/accounts/{account_holder.account_holder_uuid}",
        headers=accounts_auth_headers,
    )

    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["UUID"] == str(account_holder.account_holder_uuid)

    expected_transaction_history = [
        {
            "datetime": int(datetime(2023, 1, 1, tzinfo=timezone.utc).replace(tzinfo=None).timestamp()),
            "amount": "10.00",
            "amount_currency": "GBP",
            "location": "N/A",
            "loyalty_earned_value": "10.00",
            "loyalty_earned_type": "ACCUMULATOR",
        }
    ]

    assert resp_json["transaction_history"] == expected_transaction_history
