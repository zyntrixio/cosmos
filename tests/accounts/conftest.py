from collections.abc import Callable, Generator
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pytest

from retry_tasks_lib.db.models import RetryTask, TaskType, TaskTypeKey
from retry_tasks_lib.utils.synchronous import sync_create_task
from testfixtures import LogCapture

from cosmos.accounts.config import account_settings
from cosmos.db.models import AccountHolder, EmailTemplate, EmailTemplateKey, Retailer
from cosmos.retailers.enums import EmailTemplateTypes

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

# Top-level conftest for tests, doing things like setting up DB


@pytest.fixture(scope="function")
def test_account_holder_activation_data() -> dict:
    return {
        "email": "activate_1@test.user",
        "credentials": {
            "first_name": "Test User",
            "last_name": "Test 1",
            "date_of_birth": datetime.strptime("1970-12-03", "%Y-%m-%d").replace(tzinfo=timezone.utc).date(),
            "phone": "+447968100999",
            "address_line1": "Flat 3, Some Place",
            "address_line2": "Some Street",
            "postcode": "BN77CC",
            "city": "Brighton & Hove",
        },
    }


@pytest.fixture(scope="function")
def enrolment_callback_task_type(db_session: "Session") -> TaskType:
    task_type = TaskType(
        name=account_settings.ENROLMENT_CALLBACK_TASK_NAME,
        path="path.to.func",
        error_handler_path="path.to.error_handler",
        queue_name="queue-name",
    )
    db_session.add(task_type)
    db_session.flush()

    db_session.add_all(
        [
            TaskTypeKey(task_type_id=task_type.task_type_id, name=key_name, type=key_type)
            for key_name, key_type in (
                ("callback_url", "STRING"),
                ("account_holder_id", "INTEGER"),
                ("third_party_identifier", "STRING"),
            )
        ]
    )

    db_session.commit()
    return task_type


@pytest.fixture(scope="function")
def account_holder_activation_task_type(db_session: "Session") -> TaskType:
    task_type = TaskType(
        name=account_settings.ACCOUNT_HOLDER_ACTIVATION_TASK_NAME,
        path="path.to.func",
        error_handler_path="path.to.error_handler",
        queue_name="queue-name",
    )
    db_session.add(task_type)
    db_session.flush()

    db_session.add_all(
        [
            TaskTypeKey(task_type_id=task_type.task_type_id, name=key_name, type=key_type)
            for key_name, key_type in (
                ("account_holder_id", "INTEGER"),
                ("callback_retry_task_id", "INTEGER"),
                ("welcome_email_retry_task_id", "INTEGER"),
                ("channel", "STRING"),
                ("third_party_identifier", "STRING"),
            )
        ]
    )

    db_session.commit()
    return task_type


@pytest.fixture(scope="function")
def enrolment_callback_task(
    db_session: "Session", account_holder: AccountHolder, enrolment_callback_task_type: TaskType
) -> Generator:
    task = sync_create_task(
        task_type_name=enrolment_callback_task_type.name,
        params={
            "account_holder_id": account_holder.id,
            "callback_url": "http://callback-url/",
            "third_party_identifier": "identifier",
        },
        db_session=db_session,
    )

    db_session.add(task)
    db_session.commit()
    return task


@pytest.fixture(scope="function")
def send_email_task_type(db_session: "Session") -> TaskType:
    task_type = TaskType(
        name=account_settings.SEND_EMAIL_TASK_NAME,
        path="path.to.func",
        error_handler_path="path.to.error_handler",
        queue_name="queue-name",
    )
    db_session.add(task_type)
    db_session.flush()
    db_session.add_all(
        [
            TaskTypeKey(task_type_id=task_type.task_type_id, name=key_name, type=key_type)
            for key_name, key_type in (
                ("retailer_id", "INTEGER"),
                ("template_type", "STRING"),
                ("account_holder_id", "INTEGER"),
                ("extra_params", "JSON"),
            )
        ]
    )

    db_session.commit()
    return task_type


@pytest.fixture(scope="function")
def test_email_template_req_keys() -> list[dict]:
    return [
        {
            "name": "first_name",
            "display_name": "First name",
            "description": "Account holder first name",
        },
        {
            "name": "last_name",
            "display_name": "Last name",
            "description": "Account holder last name",
        },
        {
            "name": "account_number",
            "display_name": "Account number",
            "description": "Account holder number",
        },
        {
            "name": "marketing_opt_out_link",
            "display_name": "Marketing opt out link",
            "description": "Account holder marketing opt out link",
        },
        # {
        #     "name": "reward_url",
        #     "display_name": "Reward URL",
        #     "description": "Associated URL on account holder reward",
        # },
    ]


@pytest.fixture(scope="function")
def populate_email_template_req_keys(
    db_session: "Session", test_email_template_req_keys: list[dict]
) -> list[EmailTemplateKey]:

    email_template_req_keys = []

    for data in test_email_template_req_keys:
        email_template_key = EmailTemplateKey(**data)
        db_session.add(email_template_key)
        db_session.commit()
        email_template_req_keys.append(email_template_key)

    return email_template_req_keys


@pytest.fixture()
def create_email_template(db_session: "Session") -> Callable:
    def _create_mock_template(**email_template_params: dict) -> EmailTemplate:
        """
        Create an email template in the test DB
        :return: Callable function
        """
        mock_email_template = EmailTemplate(**email_template_params)

        db_session.add(mock_email_template)
        db_session.commit()

        return mock_email_template

    return _create_mock_template


@pytest.fixture(scope="function")
def send_welcome_email_task(
    db_session: "Session", account_holder: AccountHolder, send_email_task_type: TaskType, retailer: Retailer
) -> Generator:
    task = sync_create_task(
        task_type_name=send_email_task_type.name,
        params={
            "retailer_id": retailer.id,
            "template_type": EmailTemplateTypes.WELCOME_EMAIL.name,
            "account_holder_id": account_holder.id,
        },
        db_session=db_session,
    )

    db_session.add(task)
    db_session.commit()
    return task


@pytest.fixture(scope="function")
def account_holder_activation_task(
    db_session: "Session",
    account_holder: AccountHolder,
    account_holder_activation_task_type: TaskType,
    enrolment_callback_task: RetryTask,
    send_welcome_email_task: RetryTask,
) -> Generator:
    task = sync_create_task(
        task_type_name=account_holder_activation_task_type.name,
        params={
            "account_holder_id": account_holder.id,
            "callback_retry_task_id": enrolment_callback_task.retry_task_id,
            "welcome_email_retry_task_id": send_welcome_email_task.retry_task_id,
            "channel": "test-channel",
            "third_party_identifier": "test_3rd_perty_id",
        },
        db_session=db_session,
    )

    db_session.add(task)
    db_session.commit()
    return task


@pytest.fixture(scope="function")
def create_account_holder_activation_task(
    db_session: "Session",
    account_holder: AccountHolder,
    account_holder_activation_task_type: TaskType,
    enrolment_callback_task: RetryTask,
    send_welcome_email_task: RetryTask,
) -> Callable:
    def _create_account_holder_activation_task(**activation_task_params: dict) -> RetryTask:
        """
        Create an account holder activation retry task in the test DB
        :param activation_task_params: override any values for the activation task
        :return: Callable function
        """
        params = {
            "account_holder_id": account_holder.id,
            "callback_retry_task_id": enrolment_callback_task.retry_task_id,
            "welcome_email_retry_task_id": send_welcome_email_task.retry_task_id,
        }
        params.update(activation_task_params)
        task = sync_create_task(
            task_type_name=account_holder_activation_task_type.name,
            params=params,
            db_session=db_session,
        )

        db_session.add(task)
        db_session.commit()
        return task

    return _create_account_holder_activation_task


@pytest.fixture(scope="function")
def test_account_holder_enrol() -> dict:
    return {
        "credentials": {
            "email": "enrol_1@test.user",
            "first_name": "Test User",
            "last_name": "Test One",
            "date_of_birth": "1970-12-01",
            "phone": "+447968100999",
            "address_line1": "Flat 1, Some Place",
            "address_line2": "Some Street",
            "postcode": "BN77AA",
            "city": "Brighton & Hove",
        },
        "marketing_preferences": [
            {
                "key": "marketing_pref",
                "value": False,
            }
        ],
        "callback_url": "http://localhost:8000/whatever",
        "third_party_identifier": "whatever",
    }


@pytest.fixture(scope="function")
def test_account_holder_duplicate() -> dict:
    return {
        "credentials": {
            "email": "enrol_2@test.user",
            "first_name": "Test User",
            "last_name": "Test Two",
            "date_of_birth": "1970-12-02",
            "phone": "+447968100999",
            "address_line1": "Flat 2, Some Place",
            "address_line2": "Some Street",
            "postcode": "BN77BB",
            "city": "Brighton & Hove",
        },
        "marketing_preferences": [
            {
                "key": "marketing_pref",
                "value": False,
            }
        ],
        "callback_url": "http://localhost:8000/whatever",
        "third_party_identifier": "whatever",
    }


@pytest.fixture(scope="function")
def anonymise_account_holder_task_type(db_session: "Session") -> TaskType:
    task_type = TaskType(
        name=account_settings.ANONYMISE_ACCOUNT_HOLDER_TASK_NAME,
        path="path.to.func",
        error_handler_path="path.to.error_handler",
        queue_name="queue-name",
    )
    db_session.add(task_type)
    db_session.flush()

    db_session.add_all(
        [
            TaskTypeKey(task_type_id=task_type.task_type_id, name=key_name, type=key_type)
            for key_name, key_type in (
                ("account_holder_id", "INTEGER"),
                ("retailer_id", "INTEGER"),
            )
        ]
    )

    db_session.commit()
    return task_type


@pytest.fixture(scope="function")
def anonymise_account_holder_task(
    db_session: "Session",
    account_holder: AccountHolder,
    anonymise_account_holder_task_type: TaskType,
    retailer: Retailer,
) -> Generator:
    task = sync_create_task(
        task_type_name=anonymise_account_holder_task_type.name,
        params={"account_holder_id": account_holder.id, "retailer_id": retailer.id},
        db_session=db_session,
    )

    db_session.add(task)
    db_session.commit()
    return task


@pytest.fixture
def run_task_with_metrics() -> Generator:
    val = account_settings.core.ACTIVATE_TASKS_METRICS
    account_settings.core.ACTIVATE_TASKS_METRICS = True
    yield
    account_settings.core.ACTIVATE_TASKS_METRICS = val


@pytest.fixture(scope="function")
def capture() -> Generator:
    with LogCapture() as cpt:
        yield cpt
