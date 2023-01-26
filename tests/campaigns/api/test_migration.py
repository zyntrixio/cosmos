import math

from typing import TYPE_CHECKING, Callable
from unittest.mock import MagicMock

import pytest

from deepdiff import DeepDiff
from fastapi import status as fastapi_http_status
from pytest_mock import MockerFixture
from sqlalchemy.future import select

from cosmos.core.config import settings
from cosmos.core.error_codes import ErrorCode, ErrorCodeDetails
from cosmos.db.models import AccountHolder, CampaignBalance, PendingReward
from tests import validate_error_response
from tests.conftest import SetupType

from . import auth_headers

if TYPE_CHECKING:
    from fastapi.testclient import TestClient
    from requests import Response

    from cosmos.db.models import Campaign


def validate_list_error_response(
    resp: "Response", status_code: int, expected_payload: list[tuple[ErrorCodeDetails, list[str]]]
) -> None:
    assert resp.status_code == status_code
    expeced_error = [
        error_detail.set_optional_fields(campaigns=campaigns) for error_detail, campaigns in expected_payload
    ]
    resp_payload = resp.json()
    assert not DeepDiff(resp_payload, expeced_error, ignore_order=True)


@pytest.fixture(scope="function")
def sample_payload() -> dict:
    return {
        "to_campaign": "test-draft-campaign",
        "from_campaign": "test-active-campaign",
        "pending_rewards_action": "convert",
        "balance_action": {
            "transfer": False,
            "conversion_rate": 100,
            "qualifying_threshold": 0,
        },
        "activity_metadata": {
            "sso_username": "Test User",
        },
    }


@pytest.fixture(scope="function")
def activable_campaign(create_campaign: Callable[..., "Campaign"]) -> "Campaign":
    return create_campaign(status="DRAFT", slug="test-draft-campaign")


@pytest.fixture(scope="function")
def endable_campaign(create_campaign: Callable[..., "Campaign"]) -> "Campaign":
    return create_campaign(status="ACTIVE", slug="test-active-campaign")


def test_migration_mangled_json(test_client: "TestClient", setup: SetupType) -> None:
    retailer = setup.retailer

    resp = test_client.post(
        f"{settings.API_PREFIX}/campaigns/{retailer.slug}/migration",
        data=b"{",
        headers=auth_headers,
    )

    assert resp.status_code == fastapi_http_status.HTTP_400_BAD_REQUEST
    assert resp.json() == {
        "display_message": "Malformed request.",
        "code": "MALFORMED_REQUEST",
    }


def test_migration_invalid_token(test_client: "TestClient", setup: SetupType, campaign: "Campaign") -> None:
    retailer = setup.retailer
    resp = test_client.post(
        f"{settings.API_PREFIX}/campaigns/{retailer.slug}/migration",
        json={},
        headers={"Authorization": "Token wrong token"},
    )

    assert resp.status_code == fastapi_http_status.HTTP_401_UNAUTHORIZED
    assert resp.json() == {
        "display_message": "Supplied token is invalid.",
        "code": "INVALID_TOKEN",
    }


def test_migration_invalid_retailer(test_client: "TestClient", campaign: "Campaign") -> None:
    bad_retailer = "WRONG_RETAILER"
    resp = test_client.post(
        f"{settings.API_PREFIX}/campaigns/{bad_retailer}/migration",
        json={},
        headers=auth_headers,
    )

    validate_error_response(resp, ErrorCode.INVALID_RETAILER)


@pytest.mark.parametrize(
    ("from_campaign", "to_campaign"),
    (
        pytest.param("test-active-campaign", "WRONG-CAMPAIGN", id="draft not found"),
        pytest.param("WRONG-CAMPAIGN", "test-draft-campaign", id="active not found"),
        pytest.param("WRONG-CAMPAIGN-1", "WRONG-CAMPAIGN-2", id="both not found"),
    ),
)
def test_migration_campaign_not_found(
    from_campaign: str,
    to_campaign: str,
    test_client: "TestClient",
    sample_payload: dict,
    activable_campaign: "Campaign",
    endable_campaign: "Campaign",
) -> None:
    sample_payload["from_campaign"] = from_campaign
    sample_payload["to_campaign"] = to_campaign
    expected_not_found = [slug for slug in (from_campaign, to_campaign) if "WRONG" in slug]

    resp = test_client.post(
        f"{settings.API_PREFIX}/campaigns/{activable_campaign.retailer.slug}/migration",
        json=sample_payload,
        headers=auth_headers,
    )

    validate_list_error_response(
        resp, fastapi_http_status.HTTP_404_NOT_FOUND, [(ErrorCodeDetails.NO_CAMPAIGN_FOUND, expected_not_found)]
    )


def test_migration_campaigns_have_different_loyalty_types(
    setup: SetupType,
    test_client: "TestClient",
    sample_payload: dict,
    activable_campaign: "Campaign",
    endable_campaign: "Campaign",
) -> None:
    db_session, retailer, _ = setup

    activable_campaign.loyalty_type = "STAMPS"
    db_session.commit()

    resp = test_client.post(
        f"{settings.API_PREFIX}/campaigns/{retailer.slug}/migration",
        json=sample_payload,
        headers=auth_headers,
    )

    validate_error_response(resp, ErrorCode.INVALID_REQUEST)


@pytest.mark.parametrize(
    ("from_campaign_status", "to_campaign_status", "retailer_status"),
    (
        pytest.param("DRAFT", "DRAFT", "TEST", id="from_campaign is not ACTIVE"),
        pytest.param("ACTIVE", "ACTIVE", "TEST", id="to_campaign is not DRAFT"),
        pytest.param("DRAFT", "ACTIVE", "TEST", id="from_campaign is not ACTIVE and to_campaign is not DRAFT"),
        pytest.param("ACTIVE", "DRAFT", "ACTIVE", id="retailer is ACTIVE and there are no other active campaigns"),
    ),
)
def test_migration_invalid_status_requested(
    from_campaign_status: str,
    to_campaign_status: str,
    retailer_status: str,
    setup: SetupType,
    test_client: "TestClient",
    sample_payload: dict,
    activable_campaign: "Campaign",
    endable_campaign: "Campaign",
) -> None:
    db_session, retailer, _ = setup

    endable_campaign.status = from_campaign_status
    activable_campaign.status = to_campaign_status
    retailer.status = retailer_status
    db_session.commit()

    expected_errors: dict[ErrorCodeDetails, list[str]] = {}
    if from_campaign_status != "ACTIVE" or retailer_status != "TEST":
        expected_errors[ErrorCodeDetails.INVALID_STATUS_REQUESTED] = [endable_campaign.slug]
    if to_campaign_status != "DRAFT":
        expected_errors[ErrorCodeDetails.INVALID_STATUS_REQUESTED] = expected_errors.get(
            ErrorCodeDetails.INVALID_STATUS_REQUESTED, []
        ) + [activable_campaign.slug]
        expected_errors[ErrorCodeDetails.MISSING_CAMPAIGN_COMPONENTS] = [activable_campaign.slug]

    resp = test_client.post(
        f"{settings.API_PREFIX}/campaigns/{retailer.slug}/migration",
        json=sample_payload,
        headers=auth_headers,
    )

    validate_list_error_response(
        resp,
        fastapi_http_status.HTTP_409_CONFLICT,
        list(expected_errors.items()),
    )


@pytest.mark.parametrize(
    "rule_to_delete",
    (
        pytest.param("reward", id="to_campaign missing RewardRule"),
        pytest.param("earn", id="to_campaign missing EarnRule"),
        pytest.param("both", id="to_campaign missing RewardRule and EarnRule"),
    ),
)
def test_migration_missing_rules(
    rule_to_delete: str,
    setup: SetupType,
    test_client: "TestClient",
    sample_payload: dict,
    activable_campaign: "Campaign",
    endable_campaign: "Campaign",
) -> None:
    db_session, retailer, _ = setup
    retailer.status = "TEST"

    if rule_to_delete in {"reward", "both"}:
        db_session.delete(activable_campaign.reward_rule)
    if rule_to_delete in {"earn", "both"}:
        db_session.delete(activable_campaign.earn_rule)

    db_session.commit()

    resp = test_client.post(
        f"{settings.API_PREFIX}/campaigns/{retailer.slug}/migration",
        json=sample_payload,
        headers=auth_headers,
    )

    validate_list_error_response(
        resp,
        fastapi_http_status.HTTP_409_CONFLICT,
        [(ErrorCodeDetails.MISSING_CAMPAIGN_COMPONENTS, [activable_campaign.slug])],
    )


@pytest.mark.parametrize(
    ("pending_rewards_action", "conversion_rate", "qualifying_threshold"),
    (
        pytest.param("remove", 100, 0, id=r"remove PRs, 100% conversion_rate, 0% qualifying_threshold"),
        pytest.param("convert", 80, 0, id=r"convert PRs, 80% conversion_rate, 0% qualifying_threshold"),
        pytest.param("transfer", 100, 50, id=r"transfer PRs, 100% conversion_rate, 50% qualifying_threshold"),
        pytest.param("remove", 50, 50, id=r"remove PRs, 50% conversion_rate, 50% qualifying_threshold"),
        pytest.param("convert", 75, 30, id=r"convert PRs, 75% conversion_rate, 30% qualifying_threshold"),
        pytest.param("transfer", 100, 80, id=r"transfer PRs, 100% conversion_rate, 80% qualifying_threshold"),
    ),
)
def test_migration_ok(
    pending_rewards_action: str,
    conversion_rate: int,
    qualifying_threshold: int,
    setup: SetupType,
    mock_activity: MagicMock,
    test_client: "TestClient",
    sample_payload: dict,
    activable_campaign: "Campaign",
    endable_campaign: "Campaign",
    create_account_holder: Callable[..., AccountHolder],
    create_pending_reward: Callable[..., PendingReward],
    create_balance: Callable[..., "CampaignBalance"],
    mocker: MockerFixture,
) -> None:
    mock_convert_pr = mocker.patch("cosmos.campaigns.api.service.convert_pending_rewards_placeholder")
    db_session, retailer, account_holder_over_half = setup

    retailer.status = "TEST"
    account_holder_over_half.status = "ACTIVE"
    db_session.commit()

    account_holder_under_half = create_account_holder(email="other@account.holder")
    reward_goal: int = endable_campaign.reward_rule.reward_goal

    campaign_balance_over_half = create_balance(campaign_id=endable_campaign.id, balance=(reward_goal / 2) + 50)
    campaign_balance_under_half = create_balance(
        campaign_id=endable_campaign.id, balance=(reward_goal / 2) - 50, account_holder_id=account_holder_under_half.id
    )

    pending_reward = create_pending_reward(campaign_id=endable_campaign.id)

    sample_payload["pending_rewards_action"] = pending_rewards_action
    sample_payload["balance_action"]["transfer"] = True
    sample_payload["balance_action"]["conversion_rate"] = conversion_rate
    sample_payload["balance_action"]["qualifying_threshold"] = qualifying_threshold

    resp = test_client.post(
        f"{settings.API_PREFIX}/campaigns/{retailer.slug}/migration",
        json=sample_payload,
        headers=auth_headers,
    )

    assert resp.status_code == fastapi_http_status.HTTP_200_OK

    mock_activity.assert_called()
    assert (
        db_session.scalar(
            select(CampaignBalance).where(
                CampaignBalance.id.in_((campaign_balance_over_half.id, campaign_balance_under_half.id))
            )
        )
        is None
    )

    expected_balances: list[int] = []
    for balance in (campaign_balance_over_half.balance, campaign_balance_under_half.balance):
        if qualifying_threshold == 0 or balance >= (reward_goal / 100 * qualifying_threshold):
            expected_balances.append(math.ceil(balance / 100 * conversion_rate))
        else:
            expected_balances.append(0)

    new_balances = db_session.scalars(
        select(CampaignBalance.balance).where(CampaignBalance.campaign_id == activable_campaign.id)
    ).all()

    assert new_balances
    assert not DeepDiff(new_balances, expected_balances, ignore_order=True)

    def pending_reward_exists() -> bool:
        return db_session.scalar(select(PendingReward).where(PendingReward.id == pending_reward.id)) is not None

    match pending_rewards_action:  # noqa: E999
        case "remove":
            mock_convert_pr.assert_not_called()
            assert not pending_reward_exists()
        case "convert":
            # TODO: update this once carina logic is implemented
            mock_convert_pr.assert_called_once()
            assert pending_reward_exists()
        case "transfer":
            db_session.refresh(pending_reward)
            assert pending_reward.campaign_id == activable_campaign.id


@pytest.mark.parametrize(
    ("conversion_rate", "qualifying_threshold"),
    (
        pytest.param(100, 0, id=r"100% conversion_rate, 0% qualifying_threshold"),
        pytest.param(80, 0, id=r"80% conversion_rate, 0% qualifying_threshold"),
        pytest.param(100, 50, id=r"100% conversion_rate, 50% qualifying_threshold"),
        pytest.param(50, 50, id=r"50% conversion_rate, 50% qualifying_threshold"),
        pytest.param(75, 30, id=r"75% conversion_rate, 30% qualifying_threshold"),
        pytest.param(100, 80, id=r"100% conversion_rate, 80% qualifying_threshold"),
    ),
)
def test_migration_stamps_campaigns_ok(
    conversion_rate: int,
    qualifying_threshold: int,
    setup: SetupType,
    mock_activity: MagicMock,
    test_client: "TestClient",
    sample_payload: dict,
    activable_campaign: "Campaign",
    endable_campaign: "Campaign",
    create_account_holder: Callable[..., AccountHolder],
    create_pending_reward: Callable[..., PendingReward],
    create_balance: Callable[..., "CampaignBalance"],
) -> None:

    db_session, retailer, account_holder_over_half = setup

    retailer.status = "TEST"
    account_holder_over_half.status = "ACTIVE"
    activable_campaign.loyalty_type = "STAMPS"
    endable_campaign.loyalty_type = "STAMPS"
    db_session.commit()

    account_holder_under_half = create_account_holder(email="other@account.holder")
    reward_goal: int = endable_campaign.reward_rule.reward_goal

    campaign_balance_over_half = create_balance(campaign_id=endable_campaign.id, balance=(reward_goal / 2) + 50)
    campaign_balance_under_half = create_balance(
        campaign_id=endable_campaign.id, balance=(reward_goal / 2) - 50, account_holder_id=account_holder_under_half.id
    )

    pending_reward = create_pending_reward(campaign_id=endable_campaign.id)

    sample_payload["pending_rewards_action"] = "remove"
    sample_payload["balance_action"]["transfer"] = True
    sample_payload["balance_action"]["conversion_rate"] = conversion_rate
    sample_payload["balance_action"]["qualifying_threshold"] = qualifying_threshold

    resp = test_client.post(
        f"{settings.API_PREFIX}/campaigns/{retailer.slug}/migration",
        json=sample_payload,
        headers=auth_headers,
    )

    assert resp.status_code == fastapi_http_status.HTTP_200_OK

    mock_activity.assert_called()
    assert (
        db_session.scalar(
            select(CampaignBalance).where(
                CampaignBalance.id.in_((campaign_balance_over_half.id, campaign_balance_under_half.id))
            )
        )
        is None
    )

    expected_balances: list[int] = []
    for balance in (campaign_balance_over_half.balance, campaign_balance_under_half.balance):
        if qualifying_threshold == 0 or balance >= (reward_goal / 100 * qualifying_threshold):
            expected_balances.append(math.ceil((balance / 100 * conversion_rate) / 100) * 100)
        else:
            expected_balances.append(0)

    new_balances = db_session.scalars(
        select(CampaignBalance.balance).where(CampaignBalance.campaign_id == activable_campaign.id)
    ).all()

    assert new_balances
    assert not DeepDiff(new_balances, expected_balances, ignore_order=True)
    assert db_session.scalar(select(PendingReward).where(PendingReward.id == pending_reward.id)) is None
