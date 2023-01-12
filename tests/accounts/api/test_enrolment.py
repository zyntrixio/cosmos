import copy

from copy import deepcopy
from datetime import datetime, timezone
from unittest.mock import ANY, MagicMock, call

import yaml

from deepdiff import DeepDiff
from fastapi_prometheus_metrics.enums import EventSignals
from pytest_mock import MockerFixture
from retry_tasks_lib.db.models import RetryTask, TaskType
from retry_tasks_lib.enums import RetryTaskStatuses
from sqlalchemy.future import select
from starlette import status

from cosmos.accounts.activity.enums import ActivityType as AccountsActivityType
from cosmos.accounts.enums import AccountHolderStatuses, MarketingPreferenceValueTypes
from cosmos.core.config import settings
from cosmos.db.models import AccountHolder
from tests.accounts.fixtures import errors
from tests.conftest import SetupType

from . import accounts_auth_headers, client, validate_error_response


def test_account_holder_enrol_success(
    mocker: MockerFixture,
    setup: SetupType,
    test_account_holder_enrol: dict,
    account_holder_activation_task_type: TaskType,
    enrolment_callback_task_type: TaskType,
    send_email_task_type: TaskType,
    mock_activity: MagicMock,
) -> None:
    fake_now = datetime.now(tz=timezone.utc)
    mock_datetime = mocker.MagicMock()
    mock_datetime.now.return_value = fake_now
    mocker.patch("cosmos.accounts.api.service.datetime", mock_datetime)
    mock_signal = mocker.patch("fastapi_prometheus_metrics.middleware.signal", autospec=True)
    mock_enqueue_retry_task = mocker.patch("cosmos.core.api.tasks.enqueue_retry_task")

    db_session, retailer, _ = setup

    email = test_account_holder_enrol["credentials"]["email"]
    endpoint = f"{settings.API_PREFIX}/loyalty/%s/accounts/enrolment"
    expected_calls = [  # The expected call stack for signal, in order
        call(EventSignals.RECORD_HTTP_REQ),
        call().send(
            "fastapi_prometheus_metrics.middleware",
            endpoint=endpoint % "[retailer_slug]",
            retailer=retailer.slug,
            latency=ANY,
            response_code=status.HTTP_202_ACCEPTED,
            method="POST",
        ),
        call(EventSignals.INBOUND_HTTP_REQ),
        call().send(
            "fastapi_prometheus_metrics.middleware",
            endpoint=endpoint % "[retailer_slug]",
            retailer=retailer.slug,
            response_code=status.HTTP_202_ACCEPTED,
            method="POST",
        ),
    ]

    resp = client.post(
        endpoint % retailer.slug,
        json=test_account_holder_enrol,
        headers=accounts_auth_headers,
    )

    assert resp.status_code == status.HTTP_202_ACCEPTED
    assert resp.json() == {}
    mock_signal.assert_has_calls(expected_calls)

    account_holder: AccountHolder = db_session.execute(
        select(AccountHolder).where(
            AccountHolder.retailer_id == retailer.id,
            AccountHolder.email == email,
        )
    ).scalar_one()
    activation_task = (
        db_session.execute(
            select(RetryTask).where(
                TaskType.task_type_id == RetryTask.task_type_id,
                TaskType.name == settings.ACCOUNT_HOLDER_ACTIVATION_TASK_NAME,
            )
        )
        .unique()
        .scalar_one()
    )
    callback_task = (
        db_session.execute(
            select(RetryTask).where(
                TaskType.task_type_id == RetryTask.task_type_id,
                TaskType.name == settings.ENROLMENT_CALLBACK_TASK_NAME,
            )
        )
        .unique()
        .scalar_one_or_none()
    )
    send_welcome_email_task = (
        db_session.execute(
            select(RetryTask).where(
                TaskType.task_type_id == RetryTask.task_type_id,
                TaskType.name == settings.SEND_EMAIL_TASK_NAME,
            )
        )
        .unique()
        .scalar_one_or_none()
    )
    assert len(account_holder.marketing_preferences) == 1
    assert account_holder.marketing_preferences[0].value == "False"
    assert account_holder.marketing_preferences[0].key_name == "marketing_pref"
    assert account_holder.marketing_preferences[0].value_type == MarketingPreferenceValueTypes.BOOLEAN

    assert callback_task is not None
    assert send_welcome_email_task is not None
    assert ("retry_task_id", activation_task.retry_task_id) in mock_enqueue_retry_task.call_args.kwargs.items()

    assert account_holder is not None
    assert account_holder.account_number is None
    assert account_holder.profile is not None
    assert activation_task.status == RetryTaskStatuses.PENDING
    assert activation_task.get_params()["account_holder_id"] == account_holder.id
    assert activation_task.get_params()["callback_retry_task_id"] == callback_task.retry_task_id
    assert activation_task.get_params()["welcome_email_retry_task_id"] == send_welcome_email_task.retry_task_id

    assert callback_task.status == RetryTaskStatuses.PENDING
    assert callback_task.get_params()["third_party_identifier"] == test_account_holder_enrol["third_party_identifier"]
    assert callback_task.get_params()["callback_url"] == test_account_holder_enrol["callback_url"]
    assert callback_task.get_params()["account_holder_id"] == account_holder.id

    assert send_welcome_email_task.status == RetryTaskStatuses.PENDING
    assert send_welcome_email_task.get_params()["account_holder_id"] == account_holder.id

    assert account_holder.status == AccountHolderStatuses.PENDING
    payload = {
        "activity_datetime": fake_now,
        "channel": "channel",
        "result": "Accepted",
        "request_data": {
            "credentials": {
                "address_line1": "Flat 1, Some Place",
                "address_line2": "Some Street",
                "city": "Brighton & Hove",
                "date_of_birth": "1970-12-01",
                "email": "enrol_1@test.user",
                "first_name": "Test User",
                "last_name": "Test One",
                "phone": "+447968100999",
                "postcode": "BN77AA",
            },
            "callback_url": "http://localhost:8000/whatever",
            "marketing_preferences": [{"key": "marketing_pref", "value": False}],
            "third_party_identifier": "whatever",
        },
        "retailer_slug": "re-test",
        "retailer_profile_config": yaml.safe_load(retailer.profile_config),
    }
    mock_activity.assert_called_once_with(
        activity_type=AccountsActivityType.ACCOUNT_REQUEST,
        payload_formatter_fn=AccountsActivityType.get_account_request_activity_data,
        formatter_kwargs=payload,
    )


def test_account_holder_enrol_lower_cased_email(
    setup: SetupType,
    test_account_holder_enrol: dict,
    account_holder_activation_task_type: TaskType,
    enrolment_callback_task_type: TaskType,
    send_email_task_type: TaskType,
) -> None:
    db_session, retailer, _ = setup
    # Make a deep copy of the enrol data to be able to upper-case the email
    copy_test_account_holder_enrol: dict = deepcopy(test_account_holder_enrol)
    copy_test_account_holder_enrol["credentials"]["email"] = copy_test_account_holder_enrol["credentials"][
        "email"
    ].upper()
    email = copy_test_account_holder_enrol["credentials"]["email"]
    lower_cased_email = email.lower()
    endpoint = f"{settings.API_PREFIX}/loyalty/%s/accounts/enrolment"

    resp = client.post(
        endpoint % retailer.slug,
        json=copy_test_account_holder_enrol,
        headers=accounts_auth_headers,
    )

    assert resp.status_code == status.HTTP_202_ACCEPTED
    assert resp.json() == {}

    # Check that the lower-case email record saved OK
    account_holder = db_session.query(AccountHolder).filter_by(retailer_id=retailer.id, email=lower_cased_email).first()
    assert account_holder.email == lower_cased_email


def test_account_holder_enrol_third_party_idempty_string(
    mocker: MockerFixture,
    setup: SetupType,
    test_account_holder_enrol: dict,
    mock_activity: MagicMock,
) -> None:
    retailer = setup.retailer
    test_account_holder_enrol["third_party_identifier"] = "     "
    endpoint = f"{settings.API_PREFIX}/loyalty/{retailer.slug}/accounts/enrolment"
    resp = client.post(endpoint, json=test_account_holder_enrol, headers=accounts_auth_headers)
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert not DeepDiff(
        resp.json(),
        {
            "display_message": "Submitted fields are missing or invalid.",
            "code": "FIELD_VALIDATION_ERROR",
            "fields": ["third_party_identifier"],
        },
    )
    mock_activity.assert_not_called()


def test_account_holder_enrol_credentials_empty_string(
    mocker: MockerFixture,
    setup: SetupType,
    test_account_holder_enrol: dict,
    mock_activity: MagicMock,
) -> None:
    retailer = setup.retailer
    test_account_holder_enrol["credentials"]["address_line1"] = "     "
    test_account_holder_enrol["credentials"]["address_line2"] = "     "
    test_account_holder_enrol["credentials"]["city"] = "     "

    endpoint = f"{settings.API_PREFIX}/loyalty/{retailer.slug}/accounts/enrolment"
    resp = client.post(endpoint, json=test_account_holder_enrol, headers=accounts_auth_headers)
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert not DeepDiff(
        resp.json(),
        {
            "display_message": "Submitted fields are missing or invalid.",
            "code": "FIELD_VALIDATION_ERROR",
            "fields": ["address_line1", "address_line2", "city"],
        },
    )
    mock_activity.assert_called_once()


def test_account_holder_enrol_duplicate(
    mocker: MockerFixture,
    setup: SetupType,
    test_account_holder_duplicate: dict,
    mock_activity: MagicMock,
) -> None:
    fake_now = datetime.now(tz=timezone.utc)
    mock_datetime = mocker.MagicMock()
    mock_datetime.now.return_value = fake_now
    mocker.patch("cosmos.accounts.api.service.datetime", mock_datetime)
    mock_enqueue_retry_task = mocker.patch("cosmos.core.api.tasks.enqueue_retry_task")
    mock_signal = mocker.patch("fastapi_prometheus_metrics.middleware.signal", autospec=True)
    mock_enqueue_retry_task = mocker.patch("retry_tasks_lib.utils.asynchronous.enqueue_retry_task")

    db_session, retailer, _ = setup

    account_holder = AccountHolder(email=test_account_holder_duplicate["credentials"]["email"], retailer_id=retailer.id)

    db_session.add(account_holder)
    db_session.commit()

    endpoint = f"{settings.API_PREFIX}/loyalty/%s/accounts/enrolment"
    expected_calls = [  # The expected call stack for signal, in order
        call(EventSignals.RECORD_HTTP_REQ),
        call().send(
            "fastapi_prometheus_metrics.middleware",
            endpoint=endpoint % "[retailer_slug]",
            retailer=retailer.slug,
            latency=ANY,
            response_code=status.HTTP_409_CONFLICT,
            method="POST",
        ),
        call(EventSignals.INBOUND_HTTP_REQ),
        call().send(
            "fastapi_prometheus_metrics.middleware",
            endpoint=endpoint % "[retailer_slug]",
            retailer=retailer.slug,
            response_code=status.HTTP_409_CONFLICT,
            method="POST",
        ),
    ]

    resp = client.post(
        endpoint % retailer.slug,
        json=test_account_holder_duplicate,
        headers=accounts_auth_headers,
    )

    validate_error_response(resp, errors.ACCOUNT_EXISTS)
    mock_signal.assert_has_calls(expected_calls)
    mock_enqueue_retry_task.assert_not_called()
    payload = {
        "activity_datetime": fake_now,
        "channel": "channel",
        "result": "ACCOUNT_EXISTS",
        "request_data": {
            "credentials": {
                "address_line1": "Flat 2, Some Place",
                "address_line2": "Some Street",
                "city": "Brighton & Hove",
                "date_of_birth": "1970-12-02",
                "email": "enrol_2@test.user",
                "first_name": "Test User",
                "last_name": "Test Two",
                "phone": "+447968100999",
                "postcode": "BN77BB",
            },
            "callback_url": "http://localhost:8000/whatever",
            "marketing_preferences": [{"key": "marketing_pref", "value": False}],
            "third_party_identifier": "whatever",
        },
        "retailer_slug": "re-test",
        "retailer_profile_config": yaml.safe_load(retailer.profile_config),
    }
    mock_activity.assert_called_once_with(
        activity_type=AccountsActivityType.ACCOUNT_REQUEST,
        payload_formatter_fn=AccountsActivityType.get_account_request_activity_data,
        formatter_kwargs=payload,
    )


def test_account_holder_enrol_no_channel_header(
    mocker: MockerFixture,
    setup: SetupType,
    test_account_holder_enrol: dict,
    mock_activity: MagicMock,
) -> None:
    mock_signal = mocker.patch("fastapi_prometheus_metrics.middleware.signal", autospec=True)
    mock_enqueue_retry_task = mocker.patch("retry_tasks_lib.utils.asynchronous.enqueue_retry_task")

    retailer = setup.retailer

    endpoint = f"{settings.API_PREFIX}/loyalty/%s/accounts/enrolment"
    expected_calls = [  # The expected call stack for signal, in order
        call(EventSignals.RECORD_HTTP_REQ),
        call().send(
            "fastapi_prometheus_metrics.middleware",
            endpoint=endpoint % "[retailer_slug]",
            retailer=retailer.slug,
            latency=ANY,
            response_code=status.HTTP_400_BAD_REQUEST,
            method="POST",
        ),
        call(EventSignals.INBOUND_HTTP_REQ),
        call().send(
            "fastapi_prometheus_metrics.middleware",
            endpoint=endpoint % "[retailer_slug]",
            retailer=retailer.slug,
            response_code=status.HTTP_400_BAD_REQUEST,
            method="POST",
        ),
    ]

    resp = client.post(
        endpoint % retailer.slug,
        json=test_account_holder_enrol,
        headers={"Authorization": "Token %s" % settings.POLARIS_API_AUTH_TOKEN},
    )

    validate_error_response(resp, errors.MISSING_BPL_CHANNEL_HEADER)
    mock_signal.assert_has_calls(expected_calls)
    mock_enqueue_retry_task.assert_not_called()
    mock_activity.assert_not_called()


def test_account_holder_enrol_invalid_token(
    mocker: MockerFixture,
    setup: SetupType,
    test_account_holder_enrol: dict,
    mock_activity: MagicMock,
) -> None:
    mock_signal = mocker.patch("fastapi_prometheus_metrics.middleware.signal", autospec=True)

    retailer = setup.retailer

    endpoint = f"{settings.API_PREFIX}/loyalty/%s/accounts/enrolment"
    expected_calls = [  # The expected call stack for signal, in order
        call(EventSignals.RECORD_HTTP_REQ),
        call().send(
            "fastapi_prometheus_metrics.middleware",
            endpoint=endpoint % "[retailer_slug]",
            retailer=retailer.slug,
            latency=ANY,
            response_code=status.HTTP_401_UNAUTHORIZED,
            method="POST",
        ),
        call(EventSignals.INBOUND_HTTP_REQ),
        call().send(
            "fastapi_prometheus_metrics.middleware",
            endpoint=endpoint % "[retailer_slug]",
            retailer=retailer.slug,
            response_code=status.HTTP_401_UNAUTHORIZED,
            method="POST",
        ),
    ]

    resp = client.post(
        endpoint % retailer.slug,
        json=test_account_holder_enrol,
        # no bpl-user-channel header as unauthorized should trump all other error messages
        headers={"Authorization": "Token wrong token"},
    )

    validate_error_response(resp, errors.INVALID_TOKEN)

    mock_signal.assert_has_calls(expected_calls)

    resp = client.post(
        endpoint,
        json=test_account_holder_enrol,
        # no bpl-user-channel header as unauthorized should trump all other error messages
        headers={"Authorization": "invalid format"},
    )

    validate_error_response(resp, errors.INVALID_TOKEN)
    mock_activity.assert_not_called()


def test_account_holder_enrol_invalid_retailer(
    mocker: MockerFixture,
    test_account_holder_enrol: dict,
    mock_activity: MagicMock,
) -> None:
    mock_signal = mocker.patch("fastapi_prometheus_metrics.middleware.signal", autospec=True)
    mock_enqueue_retry_task = mocker.patch("retry_tasks_lib.utils.asynchronous.enqueue_retry_task")

    endpoint = f"{settings.API_PREFIX}/loyalty/%s/accounts/enrolment"
    expected_calls = [  # The expected call stack for signal, in order
        call(EventSignals.RECORD_HTTP_REQ),
        call().send(
            "fastapi_prometheus_metrics.middleware",
            endpoint=endpoint % "[retailer_slug]",
            retailer="WRONG_MERCHANT",
            latency=ANY,
            response_code=status.HTTP_403_FORBIDDEN,
            method="POST",
        ),
        call(EventSignals.INBOUND_HTTP_REQ),
        call().send(
            "fastapi_prometheus_metrics.middleware",
            endpoint=endpoint % "[retailer_slug]",
            retailer="WRONG_MERCHANT",
            response_code=status.HTTP_403_FORBIDDEN,
            method="POST",
        ),
    ]

    resp = client.post(
        endpoint % "WRONG_MERCHANT",
        json=test_account_holder_enrol,
        headers=accounts_auth_headers,
    )

    validate_error_response(resp, errors.INVALID_RETAILER)
    mock_signal.assert_has_calls(expected_calls)
    mock_enqueue_retry_task.assert_not_called()
    mock_activity.assert_not_called()


def test_account_holder_enrol_validation_error(
    mocker: MockerFixture,
    setup: SetupType,
    test_account_holder_enrol: dict,
    mock_activity: MagicMock,
) -> None:
    mock_signal = mocker.patch("fastapi_prometheus_metrics.middleware.signal", autospec=True)
    mock_enqueue_retry_task = mocker.patch("retry_tasks_lib.utils.asynchronous.enqueue_retry_task")

    retailer = setup.retailer

    endpoint = f"{settings.API_PREFIX}/loyalty/%s/accounts/enrolment"
    expected_calls = [  # The expected call stack for signal, in order
        call(EventSignals.RECORD_HTTP_REQ),
        call().send(
            "fastapi_prometheus_metrics.middleware",
            endpoint=endpoint % "[retailer_slug]",
            retailer=retailer.slug,
            latency=ANY,
            response_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            method="POST",
        ),
        call(EventSignals.INBOUND_HTTP_REQ),
        call().send(
            "fastapi_prometheus_metrics.middleware",
            endpoint=endpoint % "[retailer_slug]",
            retailer=retailer.slug,
            response_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            method="POST",
        ),
    ]

    wrong_payload = copy.deepcopy(test_account_holder_enrol)
    wrong_payload["credentials"]["email"] = wrong_payload["credentials"]["email"].replace("@", "!")
    wrong_payload["credentials"]["address_line1"] = "*house"
    wrong_payload["credentials"]["address_line2"] = "road!"
    wrong_payload["marketing_preferences"] = []
    del wrong_payload["credentials"]["date_of_birth"]

    resp = client.post(
        endpoint % retailer.slug,
        json=wrong_payload,
        headers=accounts_auth_headers,
    )

    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert resp.json() == {
        "display_message": "Submitted fields are missing or invalid.",
        "code": "FIELD_VALIDATION_ERROR",
        "fields": [
            "email",
            "date_of_birth",
            "address_line1",
            "address_line2",
        ],
    }

    mock_signal.assert_has_calls(expected_calls)
    mock_enqueue_retry_task.assert_not_called()
    mock_activity.assert_called_once()


def test_account_holder_enrol_missing_marketing_preferences(
    mocker: MockerFixture,
    setup: SetupType,
    test_account_holder_enrol: dict,
    mock_activity: MagicMock,
) -> None:
    mock_signal = mocker.patch("fastapi_prometheus_metrics.middleware.signal", autospec=True)
    mock_enqueue_retry_task = mocker.patch("retry_tasks_lib.utils.asynchronous.enqueue_retry_task")

    retailer = setup.retailer

    endpoint = f"{settings.API_PREFIX}/loyalty/%s/accounts/enrolment"
    expected_calls = [  # The expected call stack for signal, in order
        call(EventSignals.RECORD_HTTP_REQ),
        call().send(
            "fastapi_prometheus_metrics.middleware",
            endpoint=endpoint % "[retailer_slug]",
            retailer=retailer.slug,
            latency=ANY,
            response_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            method="POST",
        ),
        call(EventSignals.INBOUND_HTTP_REQ),
        call().send(
            "fastapi_prometheus_metrics.middleware",
            endpoint=endpoint % "[retailer_slug]",
            retailer=retailer.slug,
            response_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            method="POST",
        ),
    ]

    wrong_payload = copy.deepcopy(test_account_holder_enrol)
    wrong_payload["marketing_preferences"] = []

    resp = client.post(
        endpoint % retailer.slug,
        json=wrong_payload,
        headers=accounts_auth_headers,
    )

    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert resp.json() == {
        "display_message": "Submitted fields are missing or invalid.",
        "code": "FIELD_VALIDATION_ERROR",
        "fields": [
            "marketing_pref",
        ],
    }

    mock_signal.assert_has_calls(expected_calls)
    mock_enqueue_retry_task.assert_not_called()
    mock_activity.assert_called_once()


def test_account_holder_enrol_badly_formatted_marketing_preferences(
    mocker: MockerFixture,
    setup: SetupType,
    test_account_holder_enrol: dict,
    mock_activity: MagicMock,
) -> None:
    mock_signal = mocker.patch("fastapi_prometheus_metrics.middleware.signal", autospec=True)
    mock_enqueue_retry_task = mocker.patch("retry_tasks_lib.utils.asynchronous.enqueue_retry_task")

    retailer = setup.retailer

    endpoint = f"{settings.API_PREFIX}/loyalty/%s/accounts/enrolment"
    expected_calls = [  # The expected call stack for signal, in order
        call(EventSignals.RECORD_HTTP_REQ),
        call().send(
            "fastapi_prometheus_metrics.middleware",
            endpoint=endpoint % "[retailer_slug]",
            retailer=retailer.slug,
            latency=ANY,
            response_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            method="POST",
        ),
        call(EventSignals.INBOUND_HTTP_REQ),
        call().send(
            "fastapi_prometheus_metrics.middleware",
            endpoint=endpoint % "[retailer_slug]",
            retailer=retailer.slug,
            response_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            method="POST",
        ),
    ]

    wrong_payload = copy.deepcopy(test_account_holder_enrol)
    wrong_payload["marketing_preferences"] = [
        {
            "not-a-key": "marketing_pref",
            "noice": "very noice",
        }
    ]

    resp = client.post(
        endpoint % retailer.slug,
        json=wrong_payload,
        headers=accounts_auth_headers,
    )

    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert resp.json() == {
        "display_message": "Submitted fields are missing or invalid.",
        "code": "FIELD_VALIDATION_ERROR",
        "fields": [
            "key",
            "value",
        ],
    }

    mock_signal.assert_has_calls(expected_calls)
    mock_enqueue_retry_task.assert_not_called()
    mock_activity.assert_not_called()


def test_account_holder_enrol_marketing_preferences_wrong_value_type(
    mocker: MockerFixture,
    setup: SetupType,
    test_account_holder_enrol: dict,
    mock_activity: MagicMock,
) -> None:
    mock_signal = mocker.patch("fastapi_prometheus_metrics.middleware.signal", autospec=True)
    mock_enqueue_retry_task = mocker.patch("retry_tasks_lib.utils.asynchronous.enqueue_retry_task")

    retailer = setup.retailer

    endpoint = f"{settings.API_PREFIX}/loyalty/%s/accounts/enrolment"
    expected_calls = [  # The expected call stack for signal, in order
        call(EventSignals.RECORD_HTTP_REQ),
        call().send(
            "fastapi_prometheus_metrics.middleware",
            endpoint=endpoint % "[retailer_slug]",
            retailer=retailer.slug,
            latency=ANY,
            response_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            method="POST",
        ),
        call(EventSignals.INBOUND_HTTP_REQ),
        call().send(
            "fastapi_prometheus_metrics.middleware",
            endpoint=endpoint % "[retailer_slug]",
            retailer=retailer.slug,
            response_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            method="POST",
        ),
    ]

    wrong_payload = copy.deepcopy(test_account_holder_enrol)
    wrong_payload["marketing_preferences"] = [
        {
            "key": "marketing_pref",
            "value": "very noice",
        }
    ]

    resp = client.post(
        endpoint % retailer.slug,
        json=wrong_payload,
        headers=accounts_auth_headers,
    )

    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert resp.json() == {
        "display_message": "Submitted fields are missing or invalid.",
        "code": "FIELD_VALIDATION_ERROR",
        "fields": [
            "marketing_pref",
        ],
    }

    mock_signal.assert_has_calls(expected_calls)
    mock_enqueue_retry_task.assert_not_called()
    mock_activity.assert_called_once()
