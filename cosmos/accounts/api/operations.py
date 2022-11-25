from retry_tasks_lib.db.models import RetryTask
from retry_tasks_lib.utils.asynchronous import async_create_task
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from cosmos.accounts.enums import AccountHolderStatuses
from cosmos.core.config import settings
from cosmos.db.base_class import async_run_query
from cosmos.db.models import AccountHolder, AccountHolderMarketingPreference, AccountHolderProfile
from cosmos.retailers.enums import EmailTemplateTypes

from .enums.http_error import HttpErrors


async def enrol_account_holder(
    *,
    db_session: AsyncSession,
    email: str,
    retailer_id: int,
    callback_url: str,
    third_party_identifier: str,
    profile_data: dict,
    marketing_preferences_data: list[dict],
    channel: str,
) -> tuple[AccountHolder, RetryTask]:
    async def _query() -> tuple[AccountHolder, RetryTask]:
        account_holder = AccountHolder(email=email, retailer_id=retailer_id, status=AccountHolderStatuses.PENDING)
        nested = await db_session.begin_nested()
        try:
            db_session.add(account_holder)
            await nested.commit()
        except IntegrityError:
            await nested.rollback()
            raise HttpErrors.ACCOUNT_EXISTS.value  # pylint: disable=raise-missing-from

        profile = AccountHolderProfile(account_holder_id=account_holder.id, **profile_data)
        db_session.add(profile)
        marketing_preferences = [
            AccountHolderMarketingPreference(account_holder_id=account_holder.id, **mp)
            for mp in marketing_preferences_data
        ]
        db_session.add_all(marketing_preferences)

        callback_task = await async_create_task(
            task_type_name=settings.ENROLMENT_CALLBACK_TASK_NAME,
            db_session=db_session,
            params={
                "account_holder_id": account_holder.id,
                "callback_url": callback_url,
                "third_party_identifier": third_party_identifier,
            },
        )
        db_session.add(callback_task)
        await db_session.flush()

        welcome_email_task = await async_create_task(
            task_type_name=settings.SEND_EMAIL_TASK_NAME,
            db_session=db_session,
            params={
                "account_holder_id": account_holder.id,
                "template_type": EmailTemplateTypes.WELCOME_EMAIL.name,
                "retailer_id": retailer_id,
            },
        )
        db_session.add(welcome_email_task)
        await db_session.flush()

        activation_task = await async_create_task(
            task_type_name=settings.ACCOUNT_HOLDER_ACTIVATION_TASK_NAME,
            db_session=db_session,
            params={
                "account_holder_id": account_holder.id,
                "callback_retry_task_id": callback_task.retry_task_id,
                "welcome_email_retry_task_id": welcome_email_task.retry_task_id,
                "third_party_identifier": third_party_identifier,
                "channel": channel,
            },
        )
        db_session.add(activation_task)
        await db_session.commit()

        return account_holder, activation_task

    return await async_run_query(_query, db_session)
