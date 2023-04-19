from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from retry_tasks_lib.utils import resolve_callable_from_path
from retry_tasks_lib.utils.synchronous import enqueue_many_retry_tasks, sync_create_many_tasks
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from cosmos.core.config import core_settings, redis_raw
from cosmos.core.scheduled_tasks.scheduler import acquire_lock, cron_scheduler
from cosmos.db.models import EmailTemplate, EmailType
from cosmos.db.session import SyncSessionMaker
from cosmos.retailers.enums import EmailTypeSlugs

from . import logger

if TYPE_CHECKING:
    from cosmos.accounts.send_email_params_gen import SendEmailParams


@acquire_lock(runner=cron_scheduler)
def scheduled_balance_reset_email() -> None:
    return _scheduled_email_by_type(email_type_slug=EmailTypeSlugs.BALANCE_RESET.name)


@acquire_lock(runner=cron_scheduler)
def scheduled_purchase_prompt_email() -> None:
    return _scheduled_email_by_type(email_type_slug=EmailTypeSlugs.PURCHASE_PROMPT.name)


def _scheduled_email_by_type(*, email_type_slug: str) -> None:
    with SyncSessionMaker() as db_session:

        email_type: EmailType = (
            db_session.execute(
                select(EmailType)
                .options(joinedload(EmailType.email_templates).joinedload(EmailTemplate.retailer))
                .where(EmailType.slug == email_type_slug)
            )
            .unique()
            .scalar_one()
        )

        try:
            send_email_params_fn = resolve_callable_from_path(email_type.send_email_params_fn)
        except Exception:
            logger.exception(
                "Failed to resolve send_email_params_fn for EmailType %s (%d)", email_type.slug, email_type.id
            )
            return

        email_template: EmailTemplate
        for email_template in email_type.email_templates:

            send_task_params_list: list["SendEmailParams"] = send_email_params_fn(
                db_session=db_session,
                email_template=email_template,
                scheduler_tz=ZoneInfo(cron_scheduler.tz),
            )

            retry_tasks = sync_create_many_tasks(
                db_session,
                task_type_name=core_settings.SEND_EMAIL_TASK_NAME,
                params_list=send_task_params_list,
            )
            db_session.commit()
            if retry_tasks_ids := [rt.retry_task_id for rt in retry_tasks]:
                enqueue_many_retry_tasks(
                    db_session,
                    retry_tasks_ids=retry_tasks_ids,
                    connection=redis_raw,
                )

            logger.info(
                "%d %s %s tasks enqueued for Retailer %s.",
                len(retry_tasks_ids),
                email_type.slug,
                core_settings.SEND_EMAIL_TASK_NAME,
                email_template.retailer.slug,
            )
