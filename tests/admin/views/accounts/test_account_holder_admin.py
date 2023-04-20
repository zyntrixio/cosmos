from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest

from pytest_mock import MockerFixture
from retry_tasks_lib.utils.synchronous import sync_create_many_tasks
from sqlalchemy.exc import DataError
from sqlalchemy.future import select
from sqlalchemy.orm.exc import ObjectDeletedError
from werkzeug.datastructures import MultiDict

from admin.views.accounts import AccountHolderAdmin
from cosmos.accounts.enums import AccountHolderStatuses, MarketingPreferenceValueTypes
from cosmos.core.config import core_settings
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
from cosmos.retailers.enums import EmailTypeSlugs, RetailerStatuses
from tests.conftest import SetupType

if TYPE_CHECKING:
    from flask.testing import FlaskClient
    from retry_tasks_lib.db.models import TaskType
    from sqlalchemy.orm import Session
    from werkzeug import Response


def _fetch_account_holders(db_session: "Session") -> Sequence[AccountHolder]:
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


def test_anonymise_user_action_ok(
    setup: SetupType, test_client: "FlaskClient", mocker: MockerFixture, send_email_task_type: "TaskType"
) -> None:
    db_session, retailer, account_holder = setup

    mocker.patch.object(AccountHolderAdmin, "sso_username", "test-user")
    mocker.patch("admin.views.accounts.main.activity_scoped_session")
    mock_enqueue = mocker.patch("admin.views.accounts.main.enqueue_retry_task")
    mock_flash = mocker.patch("admin.views.accounts.main.flash")

    send_email_task_with_email, send_email_task_without_email = sync_create_many_tasks(
        db_session,
        task_type_name=core_settings.SEND_EMAIL_TASK_NAME,
        params_list=[
            {
                "account_holder_id": account_holder.id,
                "template_type": EmailTypeSlugs.WELCOME_EMAIL.name,
                "retailer_id": retailer.id,
            },
            {
                "account_holder_id": account_holder.id,
                "template_type": EmailTypeSlugs.WELCOME_EMAIL.name,
                "retailer_id": retailer.id,
            },
        ],
    )

    email = account_holder.email
    send_email_task_with_email.audit_data = [
        {
            "request": {"url": "https://api.mailjet.com/v3.1/send"},
            "response": {
                "body": {
                    "Messages": [
                        {
                            "Status": "success",
                            "CustomID": "",
                            "To": [
                                {
                                    "Email": email,
                                    "MessageUUID": "4fb97592-e2aa-4bfd-bbc1-5682b748b0d8",
                                    "MessageID": 1152921521845838572,
                                    "MessageHref": "https://api.mailjet.com/v3/REST/message/1152921521845838572",
                                }
                            ],
                            "Cc": [],
                            "Bcc": [],
                        }
                    ]
                },
                "status": 200,
            },
            "timestamp": "2023-04-20T09:12:42.127878+00:00",
        }
    ]
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

    with pytest.raises(ObjectDeletedError):
        db_session.expire(send_email_task_with_email)
        send_email_task_with_email.retry_task_id

    db_session.refresh(send_email_task_without_email)
    assert send_email_task_without_email


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
    mocker.patch(
        "admin.views.accounts.main.sync_create_task", side_effect=DataError("sample error", "test", Exception("oops"))
    )

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
