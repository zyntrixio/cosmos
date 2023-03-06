from collections.abc import Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from urllib.parse import urlparse
from uuid import UUID

import sentry_sdk

from requests.exceptions import HTTPError
from retry_tasks_lib.db.models import RetryTask
from retry_tasks_lib.enums import RetryTaskStatuses
from retry_tasks_lib.utils.synchronous import enqueue_many_retry_tasks, get_retry_task, retryable_task
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import joinedload
from sqlalchemy.orm.attributes import flag_modified

from cosmos.accounts.activity.enums import ActivityType as AccountsActivityType
from cosmos.accounts.config import account_settings
from cosmos.accounts.enums import AccountHolderStatuses
from cosmos.campaigns.enums import CampaignStatuses
from cosmos.core.activity.tasks import sync_send_activity
from cosmos.core.config import redis_raw
from cosmos.core.prometheus import task_processing_time_callback_fn, tasks_run_total
from cosmos.core.tasks import send_request_with_metrics
from cosmos.core.tasks.auth import get_callback_oauth_header
from cosmos.core.tasks.mailjet import SendEmailFalseError, send_email_to_mailjet
from cosmos.core.utils import generate_account_number
from cosmos.db.models import AccountHolder, AccountHolderEmail, Campaign, CampaignBalance, EmailTemplate, EmailType
from cosmos.db.session import SyncSessionMaker
from cosmos.retailers.enums import EmailTemplateKeys, RetailerStatuses

from . import logger

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from sqlalchemy.sql.selectable import ScalarSelect


class EmailTemplateKeysError(Exception):
    pass


def _process_callback(task_params: dict, account_holder: AccountHolder) -> dict:
    logger.info(f"Processing callback for {account_holder.account_holder_uuid}")
    timestamp = datetime.now(tz=UTC)

    if account_settings.USE_CALLBACK_OAUTH2:
        headers: dict | None = get_callback_oauth_header()
    else:
        headers = None
    parse_res = urlparse(task_params["callback_url"])
    resp = send_request_with_metrics(
        "POST",
        url_template="{scheme}://{netloc}/{path}",
        url_kwargs={"scheme": parse_res.scheme, "netloc": parse_res.netloc, "path": parse_res.path.lstrip("/")},
        exclude_from_label_url=["path"],
        json={
            "UUID": str(account_holder.account_holder_uuid),
            "email": account_holder.email,
            "account_number": account_holder.account_number,
            "third_party_identifier": task_params["third_party_identifier"],
        },
        headers=headers,
    )
    resp.raise_for_status()
    response_audit = {
        "timestamp": timestamp.isoformat(),
        "request": {"url": task_params["callback_url"]},
        "response": {"status": resp.status_code, "body": resp.text},
    }
    logger.info(f"Callback succeeded for {account_holder.account_holder_uuid}")

    return response_audit


def _activate_account_holder(
    db_session: "Session", account_holder: AccountHolder, campaigns: Sequence[Campaign]
) -> bool:
    if account_holder.account_number is None:
        while True:
            nested_trans = db_session.begin_nested()
            account_holder.account_number = generate_account_number(
                account_holder.retailer.account_number_prefix, account_holder.retailer.account_number_length
            )
            try:
                nested_trans.commit()
            except IntegrityError:
                nested_trans.rollback()
                logger.error("Card number generation triggered an IntegrityError, generating a new number.")
            else:
                break

    if (account_holder.retailer.status == RetailerStatuses.TEST) or campaigns:
        if campaigns:
            new_balances = [
                {
                    "account_holder_id": account_holder.id,
                    "campaign_id": campaign.id,
                    "balance": 0,
                    "reset_date": account_holder.retailer.current_balance_reset_date,
                }
                for campaign in campaigns
            ]
            db_session.execute(
                insert(CampaignBalance.__table__).on_conflict_do_nothing(),
                new_balances,
            )

        account_holder.status = AccountHolderStatuses.ACTIVE
        db_session.commit()

    return account_holder.status == AccountHolderStatuses.ACTIVE


# NOTE: Inter-dependency: If this function's name or module changes, ensure that
# it is relevantly reflected in the TaskType table
@retryable_task(
    db_session_factory=SyncSessionMaker,
    redis_connection=redis_raw,
    metrics_callback_fn=task_processing_time_callback_fn,
)
def account_holder_activation(retry_task: RetryTask, db_session: "Session") -> None:
    if account_settings.core.ACTIVATE_TASKS_METRICS:
        tasks_run_total.labels(
            app=account_settings.core.PROJECT_NAME, task_name=account_settings.ACCOUNT_HOLDER_ACTIVATION_TASK_NAME
        ).inc()

    task_params = retry_task.get_params()
    account_holder: AccountHolder = db_session.execute(
        select(AccountHolder).where(AccountHolder.id == task_params["account_holder_id"])
    ).scalar_one()

    logger.info(f"Getting active campaigns for {account_holder.retailer.slug}")
    campaigns: Sequence[Campaign] = (
        db_session.execute(
            select(Campaign).where(
                Campaign.retailer_id == account_holder.retailer_id, Campaign.status == CampaignStatuses.ACTIVE
            )
        )
        .scalars()
        .all()
    )

    # If there are no active campaigns for the retailer, the new account holder
    # will stay in PENDING unless the retailer is in TEST status
    if account_holder.status == AccountHolderStatuses.PENDING:
        activated = _activate_account_holder(db_session, account_holder, campaigns)

    if activated:
        activity_payload = AccountsActivityType.get_account_enrolment_activity_data(
            account_holder_uuid=account_holder.account_holder_uuid,
            retailer_slug=account_holder.retailer.slug,
            channel=task_params["channel"],
            third_party_identifier=task_params["third_party_identifier"],
            activity_datetime=account_holder.updated_at.replace(tzinfo=UTC),
        )
        sync_send_activity(activity_payload, routing_key=AccountsActivityType.ACCOUNT_ENROLMENT.value)

        welcome_email_retry_task = get_retry_task(
            db_session=db_session, retry_task_id=task_params["welcome_email_retry_task_id"]
        )
        callback_retry_task = get_retry_task(db_session=db_session, retry_task_id=task_params["callback_retry_task_id"])
        enqueue_many_retry_tasks(
            db_session=db_session,
            retry_tasks_ids=[welcome_email_retry_task.retry_task_id, callback_retry_task.retry_task_id],
            connection=redis_raw,
        )

        retry_task.update_task(db_session, status=RetryTaskStatuses.SUCCESS, clear_next_attempt_time=True)
    else:
        message = (
            f"The activation of account holder id: {account_holder.id} could not be completed due to "
            f"there being no active campaigns for the retailer {account_holder.retailer.slug}, or the "
            "account holder not being in PENDING state."
        )
        sentry_sdk.capture_message(message)

        retry_task.update_task(db_session, status=RetryTaskStatuses.WAITING, clear_next_attempt_time=True)


def extract_message_uuid_from_mailjet_response(payload: dict) -> UUID:
    """
        Extract and returns MessageID from a mailject successful response

        example response body:
        ```python
        {
            "Messages": [
                {
                    "Status": "success",
                    "CustomID": "",
                    "To": [
                        {
                            "Email": "qa+weqw_01@bink.com",
                            "MessageUUID": "01496bff-c829-4630-9ee8-887046c5438b",
                            "MessageID": 288230392991241988,
                            "MessageHref": "https://api.mailjet.com/v3/REST/message/288230392991241988",
                        }
                    ],
                    "Cc": [],
                    "Bcc": [],
                }
            ]
        }
    ```
    """

    return UUID(payload["Messages"][0]["To"][0]["MessageUUID"])


def update_account_holder_email(
    db_session: "Session", account_holder_email: AccountHolderEmail, resp_json: dict
) -> None:
    """
    Extracts MessageID from provided resp_json and adds it to the AccountHolderEmail instance
    linked to the provided retry task id.
    """
    try:
        account_holder_email.message_uuid = extract_message_uuid_from_mailjet_response(resp_json)
        db_session.commit()

    except Exception:
        logger.exception(
            "Email was sent but failed to update AccountHolderEmail.message_uuid for send-email task %d"
            "DO NOT re-enqueue. "
            "Please extract MessageID manually from response_audit and update AccountHolderEmail",
            account_holder_email.retry_task_id,
        )


# NOTE: Inter-dependency: If this function's name or module changes, ensure that
# it is relevantly reflected in the TaskType table
@retryable_task(
    db_session_factory=SyncSessionMaker,
    redis_connection=redis_raw,
    metrics_callback_fn=task_processing_time_callback_fn,
)
def enrolment_callback(retry_task: RetryTask, db_session: "Session") -> None:
    if account_settings.core.ACTIVATE_TASKS_METRICS:
        tasks_run_total.labels(
            app=account_settings.core.PROJECT_NAME, task_name=account_settings.ENROLMENT_CALLBACK_TASK_NAME
        ).inc()

    task_params = retry_task.get_params()
    account_holder: AccountHolder = db_session.execute(
        select(AccountHolder).where(AccountHolder.id == task_params["account_holder_id"])
    ).scalar_one()

    response_audit = _process_callback(task_params, account_holder)

    def _update_account_holder_and_task(db_session: "Session") -> None:
        retry_task.status = RetryTaskStatuses.SUCCESS
        retry_task.next_attempt_time = None
        retry_task.audit_data.append(response_audit)
        flag_modified(retry_task, "audit_data")
        db_session.commit()

    _update_account_holder_and_task(db_session)


def _validate_email_variables(account_holder: AccountHolder, task_params: dict, email_template: EmailTemplate) -> dict:
    email_variables = {}
    always_required_keys = {
        EmailTemplateKeys.FIRST_NAME.value: account_holder.profile.first_name.strip().title(),
        EmailTemplateKeys.LAST_NAME.value: account_holder.profile.last_name.strip().title(),
        EmailTemplateKeys.ACCOUNT_NUMBER.value: account_holder.account_number,
        EmailTemplateKeys.MARKETING_OPT_OUT_LINK.value: str(account_holder.marketing_opt_out_link),
    }
    extra_param = task_params.get("extra_params", {})
    missing_required_values = []
    # All emails are presumed to require an email address
    if not account_holder.email:
        missing_required_values.append(EmailTemplateKeys.EMAIL.value)

    for required_key in email_template.required_keys:
        if required_value := always_required_keys.get(required_key.name) or extra_param.get(required_key.name):
            email_variables[required_key.name] = required_value

        else:
            missing_required_values.append(required_key.name)
    if missing_required_values:
        raise EmailTemplateKeysError(f"Mailjet failure - missing fields: {sorted(missing_required_values)}")

    return email_variables


def _get_or_create_account_holder_email(
    db_session: "Session", *, account_holder_id: int, email_type_id: int, retry_task_id: int, campaign_slug: str | None
) -> AccountHolderEmail:
    campaign_id: "ScalarSelect | None" = (
        select(Campaign.id).where(Campaign.slug == campaign_slug).scalar_subquery() if campaign_slug else None
    )
    try:
        ahe = AccountHolderEmail(
            account_holder_id=account_holder_id,
            email_type_id=email_type_id,
            retry_task_id=retry_task_id,
            campaign_id=campaign_id,
        )
        db_session.add(ahe)
        db_session.commit()
    except IntegrityError:
        db_session.rollback()
        ahe = db_session.execute(
            select(AccountHolderEmail).where(AccountHolderEmail.retry_task_id == retry_task_id)
        ).scalar_one()

    return ahe


# NOTE: Inter-dependency: If this function's name or module changes, ensure that
# it is relevantly reflected in the TaskType table
@retryable_task(
    db_session_factory=SyncSessionMaker,
    redis_connection=redis_raw,
    metrics_callback_fn=task_processing_time_callback_fn,
)
def send_email(retry_task: RetryTask, db_session: "Session") -> None:
    """Generic email sending task"""
    response_audit: dict[str, dict | str | int | None] | str = {}
    if account_settings.core.ACTIVATE_TASKS_METRICS:
        tasks_run_total.labels(
            app=account_settings.core.PROJECT_NAME, task_name=account_settings.core.SEND_EMAIL_TASK_NAME
        ).inc()

    task_params = retry_task.get_params()
    account_holder: AccountHolder = db_session.execute(
        select(AccountHolder)
        .options(joinedload(AccountHolder.profile))
        .where(
            AccountHolder.id == task_params["account_holder_id"],
        )
    ).scalar_one()
    email_template: EmailTemplate = db_session.execute(
        select(EmailTemplate)
        .join(EmailType)
        .where(
            EmailTemplate.retailer_id == task_params["retailer_id"],
            EmailType.slug == task_params["template_type"],
        )
    ).scalar_one()
    account_holder_email = _get_or_create_account_holder_email(
        db_session,
        account_holder_id=account_holder.id,
        email_type_id=email_template.email_type_id,
        retry_task_id=retry_task.retry_task_id,
        campaign_slug=task_params.get("extra_params", {}).get("campaign_slug"),
    )

    try:
        email_variables = _validate_email_variables(
            account_holder=account_holder, task_params=task_params, email_template=email_template
        )
    except EmailTemplateKeysError as exc:
        logger.exception("Unexpected System Error", exc_info=exc)
        retry_task.update_task(db_session, status=RetryTaskStatuses.FAILED, clear_next_attempt_time=True)
    else:
        try:
            resp = send_email_to_mailjet(
                account_holder=account_holder, template_id=email_template.template_id, email_variables=email_variables
            )
        except SendEmailFalseError as ex:
            msg = "No mail sent due to SEND_MAIL=False"
            logger.warning(msg, exc_info=ex)
            response_audit = msg
        else:
            response_audit = {
                "timestamp": datetime.now(tz=UTC).isoformat(),
                "request": {"url": account_settings.core.MAILJET_API_URL},
                "response": {"status": resp.status_code, "body": resp.text},
            }
            try:
                resp.raise_for_status()
            except HTTPError as exc:
                if 400 <= exc.response.status_code < 500:  # noqa: PLR2004
                    with sentry_sdk.push_scope() as scope:
                        scope.fingerprint = ["{{ default }}", "{{ message }}"]
                        msg = (
                            f"Email error: MailJet HTTP code: {exc.response.status_code}, "
                            f"retailer slug: {account_holder.retailer.slug}, "
                            f"email type: {email_template.email_type.slug}, "
                            f"template id: {email_template.template_id}"
                        )
                        event_id = sentry_sdk.capture_message(msg)
                        logger.warning(f"{msg} (sentry event id: {event_id})")

                retry_task.update_task(
                    db_session,
                    status=RetryTaskStatuses.FAILED,
                    response_audit=response_audit,
                    clear_next_attempt_time=True,
                )
                raise

            update_account_holder_email(db_session, account_holder_email, resp.json())

        retry_task.update_task(
            db_session,
            status=RetryTaskStatuses.SUCCESS,
            response_audit=response_audit,
            clear_next_attempt_time=True,
        )
