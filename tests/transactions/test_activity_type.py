from datetime import datetime, timezone
from uuid import uuid4

import pytest

from pytest_mock import MockerFixture

from cosmos.campaigns.enums import LoyaltyTypes
from cosmos.db.models import AccountHolder, Retailer, Transaction
from cosmos.transactions.activity.enums import ActivityType
from cosmos.transactions.api.service import AdjustmentAmount


@pytest.mark.parametrize(
    ("invalid_refund", "error", "expected_reasons", "expected_summary"),
    (
        pytest.param(
            False,
            "",
            [],
            "Test Retailer Transaction Imported",
            id="Transaction imported",
        ),
        pytest.param(
            True,
            "",
            ["Refunds not supported"],
            "Test Retailer Transaction Import Failed",
            id="Refunds not supported",
        ),
        pytest.param(
            False,
            "NO_ACTIVE_CAMPAIGNS",
            ["No active campaigns"],
            "Test Retailer Transaction Import Failed",
            id="No active campaigns",
        ),
        pytest.param(
            False,
            "USER_NOT_ACTIVE",
            ["No active user"],
            "Test Retailer Transaction Import Failed",
            id="User not active",
        ),
        pytest.param(
            False,
            "DUPLICATE_TRANSACTION",
            ["Transaction ID not unique"],
            "Test Retailer Transaction Import Failed",
            id="Duplicate transaction",
        ),
        pytest.param(
            False,
            "Other error",
            ["Internal server error"],
            "Test Retailer Transaction Import Failed",
            id="Internal server error",
        ),
    ),
)
def test_tx_import_activity_payload(
    invalid_refund: bool,
    error: str,
    expected_reasons: str,
    expected_summary: str,
    retailer: Retailer,
    mocker: MockerFixture,
) -> None:
    mock_datetime = mocker.patch("cosmos.core.activity.enums.datetime")
    fake_now = datetime.now(tz=timezone.utc)
    mock_datetime.now.return_value = fake_now

    account_holder_uuid = str(uuid4())
    transaction_id = str(uuid4())
    payment_transaction_id = str(uuid4())
    transaction_datetime = datetime.now(tz=timezone.utc)
    campaign_slugs = ["slug1", "slug2"]

    assert ActivityType.get_tx_import_activity_data(
        retailer=retailer,
        campaign_slugs=campaign_slugs,
        request_payload={
            "transaction_id": transaction_id,
            "payment_transaction_id": payment_transaction_id,
            "amount": 1500,
            "transaction_datetime": transaction_datetime,
            "mid": "amid",
            "account_holder_uuid": account_holder_uuid,
        },
        error=error,
        invalid_refund=invalid_refund,
    ) == {
        "type": ActivityType.TX_IMPORT.name,
        "datetime": fake_now,
        "underlying_datetime": transaction_datetime,
        "summary": expected_summary,
        "reasons": expected_reasons,
        "activity_identifier": transaction_id,
        "user_id": account_holder_uuid,
        "associated_value": "£15.00",
        "retailer": retailer.slug,
        "campaigns": campaign_slugs,
        "data": {
            "transaction_id": transaction_id,
            "datetime": transaction_datetime,
            "amount": "15.00",
            "mid": "amid",
        },
    }


def test_get_processed_tx_activity_data(account_holder: AccountHolder, mocker: MockerFixture) -> None:
    now = datetime.now(tz=timezone.utc)
    mock_datetime = mocker.patch("cosmos.core.activity.enums.datetime")
    fake_now = datetime.now(tz=timezone.utc)
    mock_datetime.now.return_value = fake_now
    transaction = Transaction(
        retailer_id=account_holder.retailer_id,
        transaction_id="tx-id",
        amount=1000,
        mid="amid",
        datetime=now,
        processed=True,
    )
    assert ActivityType.get_processed_tx_activity_data(
        account_holder_uuid=account_holder.account_holder_uuid,
        processed_tx=transaction,
        retailer=account_holder.retailer,
        adjustment_amounts={
            "campaign": AdjustmentAmount(
                loyalty_type=LoyaltyTypes.ACCUMULATOR, amount=1000, threshold=1000, accepted=True
            )
        },
        store_name="Super store",
    ) == {
        "activity_identifier": "tx-id",
        "associated_value": "£10.00",
        "campaigns": ["campaign"],
        "data": {
            "amount": "10.00",
            "amount_currency": "GBP",
            "datetime": now,
            "earned": [{"type": "ACCUMULATOR", "value": "£10.00"}],
            "mid": "amid",
            "store_name": "Super store",
            "transaction_id": "tx-id",
        },
        "datetime": fake_now,
        "reasons": ["transaction amount £10.00 meets the required threshold £10.00"],
        "retailer": "re-test",
        "summary": "re-test Transaction Processed for Super store (MID: amid)",
        "type": "TX_HISTORY",
        "underlying_datetime": now,
        "user_id": str(account_holder.account_holder_uuid),
    }


def test_get_refund_not_recouped_activity_data(account_holder: AccountHolder, mocker: MockerFixture) -> None:
    now = datetime.now(tz=timezone.utc)
    mock_datetime = mocker.patch("cosmos.core.activity.enums.datetime")
    fake_now = datetime.now(tz=timezone.utc)
    mock_datetime.now.return_value = fake_now
    assert ActivityType.get_refund_not_recouped_activity_data(
        account_holder_uuid=account_holder.account_holder_uuid,
        activity_datetime=now,
        retailer=account_holder.retailer,
        campaigns=["campaign-a"],
        adjustment=1000,
        amount_recouped=500,
        amount_not_recouped=100,
        transaction_id="tx-id",
    ) == {
        "activity_identifier": "tx-id",
        "associated_value": "£10.00",
        "campaigns": ["campaign-a"],
        "data": {
            "amount": 1000,
            "amount_not_recouped": 100,
            "amount_recouped": 500,
            "datetime": now,
            "transaction_id": "tx-id",
        },
        "datetime": fake_now,
        "reasons": ["Account Holder Balance and/or Pending Rewards did not cover the refund"],
        "retailer": "re-test",
        "summary": "Test Retailer Refund transaction caused an account shortfall",
        "type": "REFUND_NOT_RECOUPED",
        "underlying_datetime": now,
        "user_id": str(account_holder.account_holder_uuid),
    }
