from datetime import datetime, timezone
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from fastapi import status
from pytest_mock import MockerFixture
from sqlalchemy.future import select

from cosmos.campaigns.enums import LoyaltyTypes
from cosmos.core.config import settings
from cosmos.core.error_codes import ErrorCode
from cosmos.db.models import PendingReward, Transaction
from tests import validate_error_response

from . import auth_headers

if TYPE_CHECKING:
    from unittest.mock import MagicMock

    from fastapi.testclient import TestClient

    from cosmos.db.models import Campaign, CampaignBalance
    from tests.conftest import SetupType


@pytest.fixture(scope="function")
def sample_payload() -> dict:
    return {
        "id": "TESTTXID",
        "transaction_id": str(uuid4()),
        "transaction_total": 500,
        "datetime": datetime.now(tz=timezone.utc).replace(tzinfo=None).timestamp(),
        "MID": "TSTMID",
        "loyalty_id": str(uuid4()),
    }


def test_transaction_mangled_json(test_client: "TestClient", setup: "SetupType") -> None:
    retailer = setup.retailer

    resp = test_client.post(
        f"{settings.API_PREFIX}/transactions/{retailer.slug}",
        data=b"{",
        headers=auth_headers,
    )

    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.json() == {
        "display_message": "Malformed request.",
        "code": "MALFORMED_REQUEST",
    }


def test_transaction_invalid_token(test_client: "TestClient", setup: "SetupType", campaign: "Campaign") -> None:
    retailer = setup.retailer

    resp = test_client.post(
        f"{settings.API_PREFIX}/transactions/{retailer.slug}",
        json={},
        headers={"Authorization": "Token WRONG-TOKEN"},
    )

    assert resp.status_code == status.HTTP_401_UNAUTHORIZED
    assert resp.json() == {
        "display_message": "Supplied token is invalid.",
        "code": "INVALID_TOKEN",
    }


def test_transaction_invalid_retailer(test_client: "TestClient") -> None:

    resp = test_client.post(
        f"{settings.API_PREFIX}/transactions/WRONG-RETAILER",
        json={},
        headers=auth_headers,
    )

    validate_error_response(resp, ErrorCode.INVALID_RETAILER)


def test_transaction_account_holder_not_found(
    test_client: "TestClient", setup: "SetupType", sample_payload: dict, mock_activity: "MagicMock"
) -> None:
    _, retailer, account_holder = setup

    while str(account_holder.account_holder_uuid) == sample_payload["loyalty_id"]:
        sample_payload["loyalty_id"] = str(uuid4())

    resp = test_client.post(
        f"{settings.API_PREFIX}/transactions/{retailer.slug}",
        json=sample_payload,
        headers=auth_headers,
    )

    validate_error_response(resp, ErrorCode.USER_NOT_FOUND)
    mock_activity.assert_called()


def test_transaction_user_not_active(
    test_client: "TestClient", setup: "SetupType", sample_payload: dict, mock_activity: "MagicMock"
) -> None:
    _, retailer, account_holder = setup

    assert account_holder.status != "ACTIVE"

    sample_payload["loyalty_id"] = str(account_holder.account_holder_uuid)

    resp = test_client.post(
        f"{settings.API_PREFIX}/transactions/{retailer.slug}",
        json=sample_payload,
        headers=auth_headers,
    )

    validate_error_response(resp, ErrorCode.USER_NOT_ACTIVE)
    mock_activity.assert_called()


def test_transaction_no_active_campaigns(
    test_client: "TestClient", setup: "SetupType", sample_payload: dict, mock_activity: "MagicMock"
) -> None:
    db_session, retailer, account_holder = setup

    assert not retailer.campaigns

    account_holder.status = "ACTIVE"
    db_session.commit()

    sample_payload["loyalty_id"] = str(account_holder.account_holder_uuid)

    resp = test_client.post(
        f"{settings.API_PREFIX}/transactions/{retailer.slug}",
        json=sample_payload,
        headers=auth_headers,
    )

    validate_error_response(resp, ErrorCode.NO_ACTIVE_CAMPAIGNS)
    mock_activity.assert_called()


def test_transaction_duplicated_transaction(
    test_client: "TestClient",
    setup: "SetupType",
    sample_payload: dict,
    campaign_with_rules: "Campaign",
    mock_activity: "MagicMock",
) -> None:
    db_session, retailer, account_holder = setup

    account_holder.status = "ACTIVE"
    existing_tx = Transaction(
        account_holder_id=account_holder.id,
        retailer_id=retailer.id,
        transaction_id=sample_payload["id"],
        amount=sample_payload["transaction_total"],
        mid=sample_payload["MID"],
        datetime=datetime.fromtimestamp(sample_payload["datetime"], tz=timezone.utc),
        payment_transaction_id=sample_payload["transaction_id"],
        processed=True,
    )
    db_session.add(existing_tx)
    db_session.commit()
    sample_payload["loyalty_id"] = str(account_holder.account_holder_uuid)

    resp = test_client.post(
        f"{settings.API_PREFIX}/transactions/{retailer.slug}",
        json=sample_payload,
        headers=auth_headers,
    )

    validate_error_response(resp, ErrorCode.DUPLICATE_TRANSACTION)
    assert db_session.execute(
        select(Transaction).where(
            Transaction.transaction_id == existing_tx.transaction_id,
            Transaction.retailer_id == existing_tx.retailer_id,
            Transaction.account_holder_id == existing_tx.account_holder_id,
            Transaction.processed.is_(None),
        )
    ).scalar_one()
    mock_activity.assert_called()


def test_transaction_ok_threshold_not_met(
    test_client: "TestClient",
    setup: "SetupType",
    sample_payload: dict,
    campaign_with_rules: "Campaign",
    campaign_balance: "CampaignBalance",
    mock_activity: "MagicMock",
) -> None:
    db_session, retailer, account_holder = setup
    account_holder.status = "ACTIVE"

    campaign_balance.balance = 0
    campaign_with_rules.earn_rule.threshold = sample_payload["transaction_total"] + 100
    db_session.commit()

    expected_balance = campaign_balance.balance
    sample_payload["loyalty_id"] = str(account_holder.account_holder_uuid)

    resp = test_client.post(
        f"{settings.API_PREFIX}/transactions/{retailer.slug}",
        json=sample_payload,
        headers=auth_headers,
    )
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == "Threshold not met"

    mock_activity.assert_called()
    db_session.refresh(campaign_balance)

    assert campaign_balance.balance == expected_balance
    assert not db_session.scalar(select(PendingReward).where(PendingReward.account_holder_id == account_holder.id))


def test_transaction_ok_amount_over_max(
    test_client: "TestClient",
    setup: "SetupType",
    sample_payload: dict,
    campaign_with_rules: "Campaign",
    campaign_balance: "CampaignBalance",
    mock_activity: "MagicMock",
) -> None:
    db_session, retailer, account_holder = setup
    account_holder.status = "ACTIVE"

    # if there was no max_amount the resulting pending reward should have a count of 2
    max_amount = sample_payload["transaction_total"] - 100
    reward_goal = sample_payload["transaction_total"] / 2
    campaign_balance.balance = 0
    campaign_with_rules.earn_rule.max_amount = max_amount
    campaign_with_rules.earn_rule.threshold = 0
    campaign_with_rules.reward_rule.allocation_window = 10
    campaign_with_rules.reward_rule.reward_goal = reward_goal
    db_session.commit()

    expected_balance = (campaign_balance.balance + max_amount) - reward_goal
    sample_payload["loyalty_id"] = str(account_holder.account_holder_uuid)

    resp = test_client.post(
        f"{settings.API_PREFIX}/transactions/{retailer.slug}",
        json=sample_payload,
        headers=auth_headers,
    )
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == "Awarded"

    mock_activity.assert_called()
    db_session.refresh(campaign_balance)

    assert campaign_balance.balance == expected_balance
    pr: PendingReward = db_session.scalar(
        select(PendingReward).where(PendingReward.account_holder_id == account_holder.id)
    )
    assert pr.count == 1
    assert pr.total_cost_to_user == reward_goal


@pytest.mark.parametrize(
    ("allocation_window", "threshold", "reward_goal", "tx_amount", "expected_rewards_n", "reward_cap"),
    (
        pytest.param(0, 250, 250, 400, 2, None, id="2 rewards issued"),
        pytest.param(10, 250, 250, 400, 2, None, id="pending reward with count=2 issued"),
        pytest.param(0, 100, 500, 400, 1, None, id="1 rewards issued"),
        pytest.param(10, 100, 500, 400, 1, None, id="pending reward with count=1 issued"),
        pytest.param(0, 100, 600, 400, 0, None, id="0 rewards"),
        pytest.param(10, 100, 600, 400, 0, None, id="no pending reward"),
        pytest.param(0, 100, 200, 500, 3, 2, id="rewards over cap"),
        pytest.param(10, 100, 200, 500, 3, 2, id="pending reward count over cap"),
    ),
)
def test_transaction_ok_accumulator(
    allocation_window: int,
    threshold: int,
    reward_goal: int,
    tx_amount: int,
    expected_rewards_n: int,
    reward_cap: int | None,
    test_client: "TestClient",
    setup: "SetupType",
    sample_payload: dict,
    campaign_with_rules: "Campaign",
    campaign_balance: "CampaignBalance",
    mock_activity: "MagicMock",
    mocker: MockerFixture,
) -> None:
    assert campaign_with_rules.loyalty_type == LoyaltyTypes.ACCUMULATOR

    db_session, retailer, account_holder = setup
    account_holder.status = "ACTIVE"

    mock_reward_issuance = mocker.patch("cosmos.transactions.api.service._allocate_reward_placeholder")

    campaign_balance.balance = 100
    campaign_with_rules.reward_rule.allocation_window = allocation_window
    campaign_with_rules.reward_rule.reward_goal = reward_goal
    campaign_with_rules.reward_rule.reward_cap = reward_cap
    campaign_with_rules.earn_rule.threshold = threshold

    db_session.commit()

    expected_balance = campaign_balance.balance + tx_amount
    if reward_cap and expected_rewards_n > reward_cap:
        expected_balance -= tx_amount
    elif expected_rewards_n:
        expected_balance -= reward_goal * expected_rewards_n

    sample_payload["transaction_total"] = tx_amount
    sample_payload["loyalty_id"] = str(account_holder.account_holder_uuid)

    assert not account_holder.pending_rewards

    resp = test_client.post(
        f"{settings.API_PREFIX}/transactions/{retailer.slug}",
        json=sample_payload,
        headers=auth_headers,
    )
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == "Awarded"

    mock_activity.assert_called()
    db_session.refresh(campaign_balance)

    assert campaign_balance.balance == expected_balance

    pr: PendingReward = db_session.scalar(
        select(PendingReward).where(PendingReward.account_holder_id == account_holder.id)
    )

    if allocation_window:
        mock_reward_issuance.assert_not_called()
        if expected_rewards_n:
            assert pr
            if reward_cap and expected_rewards_n > reward_cap:
                assert pr.count == reward_cap
                assert pr.total_cost_to_user == tx_amount
            else:
                assert pr.count == expected_rewards_n
                assert pr.total_cost_to_user == reward_goal * expected_rewards_n
        else:
            assert not pr
    else:
        assert not pr
        if expected_rewards_n:
            # TODO: update this once carina logic has been implemented
            mock_reward_issuance.assert_called()
        else:
            mock_reward_issuance.assert_not_called()


# refund specific adjustment logic tested in dept in tests/transactions/functional/test_refund_logic.py
@pytest.mark.parametrize(
    ("allocation_window", "expected_balance", "expected_message"),
    (
        pytest.param("10", 500, "Refund accepted", id="Refund accepted"),
        pytest.param("0", 1000, "Refunds not accepted", id="Refund not accepted"),
    ),
)
def test_transaction_refund(
    allocation_window: int,
    expected_balance: int,
    expected_message: str,
    test_client: "TestClient",
    setup: "SetupType",
    sample_payload: dict,
    campaign_with_rules: "Campaign",
    campaign_balance: "CampaignBalance",
    mock_activity: "MagicMock",
) -> None:
    assert campaign_with_rules.loyalty_type == LoyaltyTypes.ACCUMULATOR

    db_session, retailer, account_holder = setup
    account_holder.status = "ACTIVE"

    campaign_balance.balance = 1000
    campaign_with_rules.reward_rule.allocation_window = allocation_window

    db_session.commit()

    sample_payload["transaction_total"] = -500
    sample_payload["loyalty_id"] = str(account_holder.account_holder_uuid)

    assert not account_holder.pending_rewards

    resp = test_client.post(
        f"{settings.API_PREFIX}/transactions/{retailer.slug}",
        json=sample_payload,
        headers=auth_headers,
    )
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_message

    mock_activity.assert_called()
    db_session.refresh(campaign_balance)

    assert campaign_balance.balance == expected_balance


@pytest.mark.parametrize(
    ("reward_goal", "tx_amount", "increment", "expected_rewards_n", "reward_cap"),
    (
        pytest.param(300, 200, 500, 2, None, id="2 rewards issued"),
        pytest.param(500, 200, 400, 1, None, id="1 rewards issued"),
        pytest.param(600, 200, 400, 0, None, id="0 rewards"),
    ),
)
def test_transaction_ok_stamps(
    reward_goal: int,
    tx_amount: int,
    increment: int,
    expected_rewards_n: int,
    reward_cap: int | None,
    test_client: "TestClient",
    setup: "SetupType",
    sample_payload: dict,
    campaign_with_rules: "Campaign",
    campaign_balance: "CampaignBalance",
    mock_activity: "MagicMock",
    mocker: MockerFixture,
) -> None:
    assert campaign_with_rules.earn_rule.increment_multiplier == 1

    db_session, retailer, account_holder = setup
    account_holder.status = "ACTIVE"

    mock_reward_issuance = mocker.patch("cosmos.transactions.api.service._allocate_reward_placeholder")

    campaign_balance.balance = 100
    campaign_with_rules.loyalty_type = LoyaltyTypes.STAMPS
    campaign_with_rules.reward_rule.reward_goal = reward_goal
    campaign_with_rules.reward_rule.reward_cap = reward_cap
    campaign_with_rules.earn_rule.threshold = 100
    campaign_with_rules.earn_rule.increment = increment

    db_session.commit()

    expected_balance = campaign_balance.balance + increment
    if reward_cap and expected_rewards_n > reward_cap:
        expected_balance -= tx_amount
    elif expected_rewards_n:
        expected_balance -= reward_goal * expected_rewards_n

    sample_payload["transaction_total"] = tx_amount
    sample_payload["loyalty_id"] = str(account_holder.account_holder_uuid)

    assert not account_holder.pending_rewards

    resp = test_client.post(
        f"{settings.API_PREFIX}/transactions/{retailer.slug}",
        json=sample_payload,
        headers=auth_headers,
    )
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == "Awarded"

    mock_activity.assert_called()
    db_session.refresh(campaign_balance)

    assert campaign_balance.balance == expected_balance

    assert not db_session.scalar(select(PendingReward).where(PendingReward.account_holder_id == account_holder.id))

    if expected_rewards_n:
        # TODO: update this once carina logic has been implemented
        mock_reward_issuance.assert_called()
    else:
        mock_reward_issuance.assert_not_called()
