from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from pytest_mock import MockerFixture
from sqlalchemy.exc import DataError
from sqlalchemy.future import select
from werkzeug.datastructures import MultiDict

from admin.views.accounts import AccountHolderAdmin
from cosmos.accounts.enums import AccountHolderStatuses, MarketingPreferenceValueTypes
from cosmos.db.models import (
    AccountHolder,
    AccountHolderProfile,
    CampaignBalance,
    MarketingPreference,
    PendingReward,
    Retailer,
    Reward,
    Transaction,
)
from cosmos.retailers.enums import RetailerStatuses
from tests.conftest import SetupType

if TYPE_CHECKING:

    from flask.testing import FlaskClient
    from sqlalchemy.orm import Session
    from werkzeug import Response


def _fetch_account_holders(db_session: "Session") -> list[AccountHolder]:
    return db_session.execute(select(AccountHolder)).scalars().all()


def mock_delete_account_holders_request(account_holder_ids: list[str], client: "FlaskClient") -> "Response":
    return client.post(
        "/admin/account-holders/action/",
        data={
            "url": "/admin/account-holders/",
            "action": "delete-account-holder",
            "rowid": account_holder_ids,
        },
        follow_redirects=True,
    )


def test_delete_account_holder_action_invalid_retailer_status(
    setup: SetupType,
    test_client: "FlaskClient",
) -> None:
    """Tests deleting one account holder with invalid retailer status"""
    db_session, retailer, _ = setup

    retailer.status = RetailerStatuses.ACTIVE
    db_session.commit()

    fetched_account_holders = _fetch_account_holders(db_session)
    assert len(fetched_account_holders) == 1

    resp = mock_delete_account_holders_request([str(fetched_account_holders[0].id)], test_client)

    assert resp.status_code == 200
    assert (
        "This action is allowed only for account holders that belong to a TEST retailer." in resp.text  # type: ignore
    )

    re_fetched_account_holders = _fetch_account_holders(db_session)
    assert len(re_fetched_account_holders) == 1


def test_delete_account_holder_action_success_with_cascades(
    setup: SetupType,
    test_client: "FlaskClient",
    mocker: MockerFixture,
    user_reward: Reward,
    campaign_balance: CampaignBalance,
    pending_reward: PendingReward,
) -> None:
    """Tests deleting one account holder and checking for all cascade deletes"""
    db_session, retailer, account_holder = setup

    mocker.patch.object(AccountHolderAdmin, "sso_username", "test-user")
    mock_send_activity = mocker.patch("admin.views.accounts.main.sync_send_activity")

    user_reward.account_holder_id = account_holder.id
    tx = Transaction(
        account_holder_id=account_holder.id,
        retailer_id=retailer.id,
        transaction_id="tx_id",
        amount=300,
        mid="mid",
        datetime=datetime.now(tz=UTC),
    )
    prefs = MarketingPreference(
        account_holder_id=account_holder.id,
        key_name="marketing_pref",
        value="True",
        value_type=MarketingPreferenceValueTypes.BOOLEAN,
    )
    db_session.add(tx)
    db_session.add(prefs)
    db_session.commit()

    fetched_account_holders = _fetch_account_holders(db_session)
    assert len(fetched_account_holders) == 1
    assert fetched_account_holders[0].profile is not None
    assert fetched_account_holders[0].pending_rewards is not None
    assert fetched_account_holders[0].rewards is not None
    assert fetched_account_holders[0].current_balances is not None
    assert fetched_account_holders[0].transactions is not None
    assert fetched_account_holders[0].marketing_preferences is not None
    assert fetched_account_holders[0].retailer.status == RetailerStatuses.TEST

    resp = mock_delete_account_holders_request([str(fetched_account_holders[0].id)], test_client)

    assert resp.status_code == 200
    assert f"Deleted {len(fetched_account_holders)} Account Holders." in resp.text  # type: ignore

    re_fetched_account_holders = _fetch_account_holders(db_session)
    assert not re_fetched_account_holders

    # Check for cascade deletes
    for table in (AccountHolderProfile, PendingReward, Reward, CampaignBalance, Transaction, MarketingPreference):
        assert len(db_session.execute(select(table)).scalars().all()) == 0

    # Check activity was sent
    mock_send_activity.assert_called_once()


def test_delete_account_holder_action_multiple(
    setup: SetupType,
    test_client: "FlaskClient",
    mocker: MockerFixture,
    create_account_holder: Callable[..., AccountHolder],
) -> None:
    """Tests deleting multiple account holders successfully"""

    db_session, _, _ = setup

    mocker.patch.object(AccountHolderAdmin, "sso_username", "test-user")
    mock_send_activity = mocker.patch("admin.views.accounts.main.sync_send_activity")

    create_account_holder(email="other@account.holder")

    fetched_account_holders = _fetch_account_holders(db_session)
    assert len(fetched_account_holders) == 2

    resp = mock_delete_account_holders_request(
        [str(fetched_account_holders[0].id), str(fetched_account_holders[1].id)], test_client
    )

    assert resp.status_code == 200
    assert f"Deleted {len(fetched_account_holders)} Account Holders." in resp.text  # type: ignore

    re_fetched_account_holders = _fetch_account_holders(db_session)
    assert not re_fetched_account_holders

    # Check activity was sent
    mock_send_activity.assert_called_once()


def test_delete_account_holder_action_multiple_with_one_invalid(
    setup: SetupType,
    test_client: "FlaskClient",
    mocker: MockerFixture,
    create_account_holder: Callable[..., AccountHolder],
    create_retailer: Callable[..., Retailer],
) -> None:
    """Tests deleting one account with active retailer, one account with test retailer"""

    db_session, _, _ = setup

    mocker.patch.object(AccountHolderAdmin, "sso_username", "test-user")
    mock_send_activity = mocker.patch("admin.views.accounts.main.sync_send_activity")

    new_retailer = create_retailer(name="new retailer", slug="new-retailer", status=RetailerStatuses.ACTIVE)
    create_account_holder(email="other@account.holder", retailer_id=new_retailer.id)

    fetched_account_holders = _fetch_account_holders(db_session)
    assert len(fetched_account_holders) == 2

    resp = mock_delete_account_holders_request(
        [str(fetched_account_holders[0].id), str(fetched_account_holders[1].id)], test_client
    )

    assert resp.status_code == 200
    assert "Deleted 1 Account Holders." in resp.text  # type: ignore

    re_fetched_account_holders = _fetch_account_holders(db_session)
    assert len(re_fetched_account_holders) == 1

    # Check activity was sent
    mock_send_activity.assert_called_once()


def test_anonymise_user_action_ok(setup: SetupType, test_client: "FlaskClient", mocker: MockerFixture) -> None:
    db_session, _, account_holder = setup
    mocker.patch.object(AccountHolderAdmin, "sso_username", "test-user")
    mocker.patch("admin.views.accounts.main.activity_scoped_session")
    mock_enqueue = mocker.patch("admin.views.accounts.main.enqueue_retry_task")
    mock_flash = mocker.patch("admin.views.accounts.main.flash")

    email = account_holder.email
    account_number = "TEST1234"
    assert account_holder.profile

    account_holder.status = AccountHolderStatuses.ACTIVE
    account_holder.account_number = account_number
    db_session.commit()

    resp = test_client.post(
        "/admin/account-holders/action/",
        data={
            "url": "/admin/account-holders/",
            "action": "anonymise-account-holder",
            "rowid": account_holder.id,
        },
        follow_redirects=True,
    )

    assert resp.status_code == 200

    db_session.refresh(account_holder)
    assert account_holder.email != email
    assert account_holder.account_number != account_number
    assert account_holder.profile is None
    assert account_holder.status == AccountHolderStatuses.INACTIVE

    mock_enqueue.assert_called_once()
    mock_flash.assert_called_once_with(f"Account Holder (id: {account_holder.id}) successfully anonymised.")


def test_anonymise_user_action_too_many_selected(
    setup: SetupType, test_client: "FlaskClient", mocker: MockerFixture
) -> None:
    db_session, _, account_holder = setup
    mocker.patch.object(AccountHolderAdmin, "sso_username", "test-user")
    mocker.patch("admin.views.accounts.main.activity_scoped_session")
    mock_enqueue = mocker.patch("admin.views.accounts.main.enqueue_retry_task")
    mock_flash = mocker.patch("admin.views.accounts.main.flash")

    email = account_holder.email
    account_number = "TEST1234"
    assert account_holder.profile

    account_holder.account_number = account_number
    db_session.commit()

    resp = test_client.post(
        "/admin/account-holders/action/",
        data=MultiDict(
            (
                ("url", "/admin/account-holders/"),
                ("action", "anonymise-account-holder"),
                ("rowid", account_holder.id),
                ("rowid", account_holder.id + 1),
            )
        ),
        follow_redirects=True,
    )

    assert resp.status_code == 200

    db_session.refresh(account_holder)
    assert account_holder.email == email
    assert account_holder.account_number == account_number
    assert account_holder.profile

    mock_enqueue.assert_not_called()
    mock_flash.assert_called_once_with(
        "This action must be completed for account holders one at a time", category="error"
    )


def test_anonymise_user_action_account_holder_inactive(
    setup: SetupType, test_client: "FlaskClient", mocker: MockerFixture
) -> None:
    db_session, _, account_holder = setup
    mocker.patch.object(AccountHolderAdmin, "sso_username", "test-user")
    mocker.patch("admin.views.accounts.main.activity_scoped_session")
    mock_enqueue = mocker.patch("admin.views.accounts.main.enqueue_retry_task")
    mock_flash = mocker.patch("admin.views.accounts.main.flash")

    email = account_holder.email
    account_number = "TEST1234"
    assert account_holder.profile

    account_holder.status = AccountHolderStatuses.INACTIVE
    account_holder.account_number = account_number
    db_session.commit()

    resp = test_client.post(
        "/admin/account-holders/action/",
        data={
            "url": "/admin/account-holders/",
            "action": "anonymise-account-holder",
            "rowid": account_holder.id,
        },
        follow_redirects=True,
    )

    assert resp.status_code == 200

    db_session.refresh(account_holder)
    assert account_holder.email == email
    assert account_holder.account_number == account_number
    assert account_holder.profile
    assert account_holder.status == AccountHolderStatuses.INACTIVE

    mock_enqueue.assert_not_called()
    mock_flash.assert_called_once_with("Account holder is INACTIVE", category="error")


def test_anonymise_user_action_account_holder_not_found(test_client: "FlaskClient", mocker: MockerFixture) -> None:

    mocker.patch.object(AccountHolderAdmin, "sso_username", "test-user")
    mocker.patch("admin.views.accounts.main.activity_scoped_session")
    mock_enqueue = mocker.patch("admin.views.accounts.main.enqueue_retry_task")
    mock_flash = mocker.patch("admin.views.accounts.main.flash")

    resp = test_client.post(
        "/admin/account-holders/action/",
        data={
            "url": "/admin/account-holders/",
            "action": "anonymise-account-holder",
            "rowid": 12,
        },
        follow_redirects=True,
    )

    assert resp.status_code == 200

    mock_enqueue.assert_not_called()
    mock_flash.assert_called_once_with("Account holder not found", category="error")


def test_anonymise_user_action_db_error(setup: SetupType, test_client: "FlaskClient", mocker: MockerFixture) -> None:
    db_session, _, account_holder = setup
    mocker.patch.object(AccountHolderAdmin, "sso_username", "test-user")
    mocker.patch("admin.views.accounts.main.activity_scoped_session")
    mock_enqueue = mocker.patch("admin.views.accounts.main.enqueue_retry_task")
    mock_flash = mocker.patch("admin.views.accounts.main.flash")
    mocker.patch("admin.views.accounts.main.sync_create_task", side_effect=DataError("sample error", "test", "test"))

    email = account_holder.email
    account_number = "TEST1234"
    assert account_holder.profile

    account_holder.status = AccountHolderStatuses.ACTIVE
    account_holder.account_number = account_number
    db_session.commit()

    resp = test_client.post(
        "/admin/account-holders/action/",
        data={
            "url": "/admin/account-holders/",
            "action": "anonymise-account-holder",
            "rowid": account_holder.id,
        },
        follow_redirects=True,
    )

    assert resp.status_code == 200

    db_session.refresh(account_holder)
    assert account_holder.email == email
    assert account_holder.account_number == account_number
    assert account_holder.profile
    assert account_holder.status == AccountHolderStatuses.ACTIVE

    mock_enqueue.assert_not_called()
    mock_flash.assert_called_once_with(
        f"Failed to anonymise Account Holder (id: {account_holder.id}), rolling back.", category="error"
    )
