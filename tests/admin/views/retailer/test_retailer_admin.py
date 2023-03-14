import json

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import pytest
import wtforms

from flask import url_for
from pytest_mock import MockerFixture
from sqlalchemy.future import select
from werkzeug.datastructures import MultiDict

from admin.views.retailer import RetailerAdmin
from admin.views.retailer.validators import validate_balance_lifespan_and_warning_days
from cosmos.campaigns.enums import CampaignStatuses
from cosmos.db.models import AccountHolder, AccountHolderProfile, Campaign, Retailer, Reward, Transaction
from cosmos.retailers.enums import RetailerStatuses

if TYPE_CHECKING:
    from flask.testing import FlaskClient
    from sqlalchemy.orm import Session

    from tests.conftest import SetupType


def _fetch_retailers(db_session: "Session") -> list[Retailer]:
    return db_session.execute(select(Retailer)).scalars().all()


def test_delete_account_holder_action_invalid_retailer_status(
    setup: "SetupType",
    create_retailer: Callable[..., Retailer],
    test_client: "FlaskClient",
) -> None:
    """Tests trying to delete more than one retailer"""
    db_session, retailer, _ = setup

    retailer.status = RetailerStatuses.ACTIVE
    new_retailer = create_retailer(name="new retailer", slug="new-retailer", status=RetailerStatuses.ACTIVE)
    db_session.commit()

    fetched_retailers = _fetch_retailers(db_session)
    assert len(fetched_retailers) == 2

    resp = test_client.get(
        f"/admin/retailers/custom-actions/delete-retailer?ids={retailer.id}&ids={new_retailer.id}",
        follow_redirects=True,
    )

    assert resp.status_code == 200
    assert "Only one Retailer allowed for this action" in resp.text

    refetched_retailers = _fetch_retailers(db_session)
    assert len(refetched_retailers) == 2


def test_delete_retailer_action_success_with_cascades(
    setup: "SetupType",
    test_client: "FlaskClient",
    mocker: MockerFixture,
    user_reward: Reward,
) -> None:
    """Tests deleting a retailer and checking for all cascade deletes"""
    db_session, retailer, account_holder = setup

    mocker.patch.object(RetailerAdmin, "sso_username", "test-user")
    mocker.patch("admin.views.retailer.custom_actions.activity_scoped_session")

    mock_send_activity = mocker.patch("admin.views.retailer.main.sync_send_activity")
    flash = mocker.patch("admin.views.retailer.custom_actions.flash")

    user_reward.account_holder_id = account_holder.id
    tx = Transaction(
        account_holder_id=account_holder.id,
        retailer_id=retailer.id,
        transaction_id="tx_id",
        amount=300,
        mid="mid",
        datetime=datetime.now(tz=UTC),
    )
    db_session.add(tx)
    db_session.commit()

    fetched_retailers = _fetch_retailers(db_session)
    assert len(fetched_retailers) == 1
    assert fetched_retailers[0].account_holders is not None
    assert fetched_retailers[0].transactions is not None
    assert fetched_retailers[0].campaigns is not None
    assert fetched_retailers[0].status == RetailerStatuses.TEST

    resp = test_client.get(
        f"/admin/retailers/custom-actions/delete-retailer?ids={retailer.id}",
        follow_redirects=True,
    )
    assert resp.status_code == 200

    resp = test_client.post(
        f"/admin/retailers/custom-actions/delete-retailer?ids={retailer.id}",
        data={
            "acceptance": True,
        },
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert f"All rows related to retailer {retailer.name} ({retailer.id}) have been deleted." == flash.call_args.args[0]

    re_fetched_account_holders = _fetch_retailers(db_session)
    assert not re_fetched_account_holders

    # Check for cascade deletes
    for table in (
        AccountHolderProfile,
        Transaction,
        AccountHolder,
        Campaign,
        Reward,
        Transaction,
    ):
        assert len(db_session.execute(select(table)).scalars().all()) == 0

    # Check activity was sent
    mock_send_activity.assert_called_once()


def test_delete_retailer_action_active_retailer(
    setup: "SetupType",
    test_client: "FlaskClient",
    mocker: MockerFixture,
    user_reward: Reward,
) -> None:
    """Tests deleting a retailer and checking for all cascade deletes"""
    db_session, retailer, account_holder = setup

    mocker.patch.object(RetailerAdmin, "sso_username", "test-user")
    mock_send_activity = mocker.patch("admin.views.retailer.main.sync_send_activity")
    flash = mocker.patch("admin.views.retailer.main.flash")

    user_reward.account_holder_id = account_holder.id
    retailer.status = RetailerStatuses.ACTIVE
    tx = Transaction(
        account_holder_id=account_holder.id,
        retailer_id=retailer.id,
        transaction_id="tx_id",
        amount=300,
        mid="mid",
        datetime=datetime.now(tz=UTC),
    )
    db_session.add(tx)
    db_session.commit()

    fetched_retailers = _fetch_retailers(db_session)
    assert fetched_retailers[0].status == RetailerStatuses.ACTIVE

    resp = test_client.get(
        f"/admin/retailers/custom-actions/delete-retailer?ids={retailer.id}",
        follow_redirects=True,
    )
    assert resp.status_code == 200

    resp = test_client.post(
        f"/admin/retailers/custom-actions/delete-retailer?ids={retailer.id}",
        data={
            "acceptance": True,
        },
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert flash.call_args.args[0] == "Only non active Retailers allowed for this action"

    re_fetched_account_holders = _fetch_retailers(db_session)
    assert re_fetched_account_holders

    # Check for cascade deletes
    for table in (
        AccountHolderProfile,
        Transaction,
        AccountHolder,
        Campaign,
        Reward,
        Transaction,
    ):
        assert len(db_session.execute(select(table)).scalars().all()) == 1

    # Check activity was not sent
    mock_send_activity.assert_not_called()


def test_delete_retailer_action_no_acceptance(
    setup: "SetupType",
    test_client: "FlaskClient",
    mocker: MockerFixture,
    user_reward: Reward,
) -> None:
    """Tests deleting a retailer and checking for all cascade deletes"""
    db_session, retailer, account_holder = setup

    mocker.patch.object(RetailerAdmin, "sso_username", "test-user")
    mock_send_activity = mocker.patch("admin.views.retailer.main.sync_send_activity")
    flash = mocker.patch("admin.views.retailer.custom_actions.flash")

    user_reward.account_holder_id = account_holder.id
    tx = Transaction(
        account_holder_id=account_holder.id,
        retailer_id=retailer.id,
        transaction_id="tx_id",
        amount=300,
        mid="mid",
        datetime=datetime.now(tz=UTC),
    )
    db_session.add(tx)
    db_session.commit()

    resp = test_client.get(
        f"/admin/retailers/custom-actions/delete-retailer?ids={retailer.id}",
        follow_redirects=True,
    )
    assert resp.status_code == 200

    resp = test_client.post(
        f"/admin/retailers/custom-actions/delete-retailer?ids={retailer.id}",
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert flash.call_args.args[0] == "User did not agree to proceed, action halted."

    re_fetched_account_holders = _fetch_retailers(db_session)
    assert re_fetched_account_holders

    # Check for cascade deletes
    for table in (
        AccountHolderProfile,
        Transaction,
        AccountHolder,
        Campaign,
        Reward,
        Transaction,
    ):
        assert len(db_session.execute(select(table)).scalars().all()) == 1

    # Check activity was not sent
    mock_send_activity.assert_not_called()


def test_activate_retailer_more_than_one_selected(
    setup: "SetupType", test_client: "FlaskClient", mocker: MockerFixture
) -> None:
    db_session, retailer, _ = setup
    assert retailer.status == RetailerStatuses.TEST

    mocker.patch.object(RetailerAdmin, "sso_username", "test-user")
    mock_send_activity = mocker.patch("admin.views.retailer.main.sync_send_activity")
    mock_flash = mocker.patch("admin.views.retailer.main.flash")

    resp = test_client.post(
        "/admin/retailers/action",
        data=MultiDict(
            (
                ("url", "/admin/retailers/"),
                ("action", "activate retailer"),
                ("rowid", retailer.id),
                ("rowid", retailer.id + 1),
            )
        ),
        follow_redirects=True,
    )
    assert resp.status_code == 200

    db_session.refresh(retailer)
    assert retailer.status == RetailerStatuses.TEST

    mock_send_activity.assert_not_called()
    mock_flash.assert_called_once_with("Cannot activate more than one retailer at once", category="error")


def test_activate_retailer_retailer_not_test(
    setup: "SetupType", test_client: "FlaskClient", mocker: MockerFixture
) -> None:
    db_session, retailer, _ = setup
    retailer.status = RetailerStatuses.INACTIVE
    db_session.commit()

    mocker.patch.object(RetailerAdmin, "sso_username", "test-user")
    mock_send_activity = mocker.patch("admin.views.retailer.main.sync_send_activity")
    mock_flash = mocker.patch("admin.views.retailer.main.flash")

    resp = test_client.post(
        "/admin/retailers/action",
        data={"url": "/admin/retailers/", "action": "activate retailer", "rowid": retailer.id},
        follow_redirects=True,
    )
    assert resp.status_code == 200

    db_session.refresh(retailer)
    assert retailer.status == RetailerStatuses.INACTIVE

    mock_send_activity.assert_not_called()
    mock_flash.assert_called_once_with("Retailer in incorrect state for activation", category="error")


def test_activate_retailer_no_active_campaigns(
    setup: "SetupType", test_client: "FlaskClient", mocker: MockerFixture
) -> None:
    db_session, retailer, _ = setup
    assert retailer.status == RetailerStatuses.TEST

    mocker.patch.object(RetailerAdmin, "sso_username", "test-user")
    mock_send_activity = mocker.patch("admin.views.retailer.main.sync_send_activity")
    mock_flash = mocker.patch("admin.views.retailer.main.flash")

    resp = test_client.post(
        "/admin/retailers/action",
        data={"url": "/admin/retailers/", "action": "activate retailer", "rowid": retailer.id},
        follow_redirects=True,
    )
    assert resp.status_code == 200

    db_session.refresh(retailer)
    assert retailer.status == RetailerStatuses.TEST

    mock_send_activity.assert_not_called()
    mock_flash.assert_called_once_with("Retailer has no active campaign", category="error")


def test_activate_retailer_ok(
    setup: "SetupType", campaign_with_rules: Campaign, test_client: "FlaskClient", mocker: MockerFixture
) -> None:
    db_session, retailer, _ = setup
    assert retailer.status == RetailerStatuses.TEST

    campaign_with_rules.status = CampaignStatuses.ACTIVE
    db_session.commit()

    mocker.patch.object(RetailerAdmin, "sso_username", "test-user")
    mock_send_activity = mocker.patch("admin.views.retailer.main.sync_send_activity")
    mock_flash = mocker.patch("admin.views.retailer.main.flash")

    resp = test_client.post(
        "/admin/retailers/action",
        data={"url": "/admin/retailers/", "action": "activate retailer", "rowid": retailer.id},
        follow_redirects=True,
    )
    assert resp.status_code == 200

    db_session.refresh(retailer)
    assert retailer.status == RetailerStatuses.ACTIVE

    mock_send_activity.assert_called_once()
    mock_flash.assert_called_once_with("Update retailer status successfully")


@dataclass
class SetupData:
    original_warning_days: int | None
    new_warning_days: int | None
    status: RetailerStatuses
    balance_lifespan: int | None


@dataclass
class ExpectationData:
    response: str | None


test_data = [
    [
        "balance_reset_advanced_warning_days more than balance_lifespan",
        SetupData(
            original_warning_days=None,
            new_warning_days=30,
            status=RetailerStatuses.TEST,
            balance_lifespan=20,
        ),
        ExpectationData(response="The balance_reset_advanced_warning_days must be less than the balance_lifespan"),
    ],
    [
        "balance_reset_advanced_warning_days are manditory if the balance_lifespan is set",
        SetupData(
            original_warning_days=None,
            new_warning_days=None,
            status=RetailerStatuses.TEST,
            balance_lifespan=20,
        ),
        ExpectationData(response="You must set both the balance_lifespan with the balance_reset_advanced_warning_days"),
    ],
    [
        "not able to update the balance_reset_advanced_warning_days for an active retailer",
        SetupData(
            original_warning_days=7,
            new_warning_days=10,
            status=RetailerStatuses.ACTIVE,
            balance_lifespan=30,
        ),
        ExpectationData(response="You cannot update the balance_reset_advanced_warning_days for an active retailer"),
    ],
    [
        "not able to set a balance_lifespan without a balance_reset_advanced_warning_days for ACTIVE retailers",
        SetupData(
            original_warning_days=None,
            new_warning_days=None,
            status=RetailerStatuses.ACTIVE,
            balance_lifespan=30,
        ),
        ExpectationData(response="You must set both the balance_lifespan with the balance_reset_advanced_warning_days"),
    ],
    [
        "not able to set a balance_lifespan without a balance_reset_advanced_warning_days for TEST retailers",
        SetupData(
            original_warning_days=None,
            new_warning_days=None,
            status=RetailerStatuses.TEST,
            balance_lifespan=30,
        ),
        ExpectationData(response="You must set both the balance_lifespan with the balance_reset_advanced_warning_days"),
    ],
    [
        "not able to have balance_reset_advanced_warning_days without a balance_lifespan",
        SetupData(
            original_warning_days=None,
            new_warning_days=7,
            status=RetailerStatuses.TEST,
            balance_lifespan=None,
        ),
        ExpectationData(response="You must set both the balance_lifespan with the balance_reset_advanced_warning_days"),
    ],
]


@pytest.mark.parametrize(
    "_description,setup_data,expectation_data",
    test_data,
    ids=[f"{i[0]}" for i in test_data],
)
def test_validate_balance_reset_advanced_warning_days(
    _description: str,
    setup_data: SetupData,
    expectation_data: ExpectationData,
) -> None:
    class MockForm:
        def __init__(self, data: Any) -> None:  # noqa: ANN401
            self.__dict__.update(data)

    def build_form(data: dict) -> Any:  # noqa: ANN401
        return json.loads(json.dumps(data), object_hook=MockForm)

    data = {
        "balance_reset_advanced_warning_days": {
            "data": setup_data.new_warning_days,
            "object_data": setup_data.original_warning_days,
        },
        "balance_lifespan": {"data": setup_data.balance_lifespan},
    }

    mock_form: wtforms.Form = build_form(data)
    retailer_status = setup_data.status
    with pytest.raises(wtforms.ValidationError) as exc_info:
        validate_balance_lifespan_and_warning_days(mock_form, retailer_status)
    assert exc_info.value.args[0] == expectation_data.response


@pytest.mark.parametrize("params", [[0, None], [None, 0]])
def test_create_retailer_with_balance_lifespan_set_to_zero(
    test_client: "FlaskClient", db_session: "Session", params: list
) -> None:
    balance_lifespan, warning_days = params
    resp = test_client.post(
        url_for("retailers.create_view"),
        data={
            "name": "Test Retailer",
            "slug": "test-retailer",
            "account_number_prefix": "TEST",
            "profile_config": """email:
  required: true
  label: Email address
first_name:
  required: true
  label: Forename
last_name:
  required: true
  label: Surname""",
            "marketing_preference_config": """marketing_pref:
  label: Spam?
  type: boolean""",
            "loyalty_name": "test",
            "status": "TEST",
            "balance_lifespan": balance_lifespan,
            "balance_reset_advanced_warning_days": warning_days,
        },
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert "Number must be at least 1" in resp.text
    assert not db_session.scalars(select(Retailer).where(Retailer.slug == "test-retailer")).all()
