from collections.abc import Callable
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from pytest_mock import MockerFixture
from sqlalchemy.future import select
from werkzeug.datastructures import MultiDict

from admin.views.retailer import RetailerAdmin
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
    mocker.patch("admin.views.retailer.custom_actions.hubble_db_session")

    mock_send_activity = mocker.patch("admin.views.retailer.main.sync_send_activity")
    flash = mocker.patch("admin.views.retailer.custom_actions.flash")

    user_reward.account_holder_id = account_holder.id
    tx = Transaction(
        account_holder_id=account_holder.id,
        retailer_id=retailer.id,
        transaction_id="tx_id",
        amount=300,
        mid="mid",
        datetime=datetime.now(tz=timezone.utc),
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
        datetime=datetime.now(tz=timezone.utc),
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
        datetime=datetime.now(tz=timezone.utc),
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
