from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from retry_tasks_lib.db.models import RetryTask, RetryTaskStatuses

from cosmos.core.scheduled_tasks.scheduler import acquire_lock, cron_scheduler
from cosmos.db.session import SyncSessionMaker

from . import logger

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


@acquire_lock(runner=cron_scheduler)
def cleanup_old_tasks() -> None:
    # today at midnight - 6 * 30 days (circa 6 months ago)
    time_reference = datetime.now(tz=UTC).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=180)
    # tasks in a successful terminal state
    deleteable_task_statuses = {
        RetryTaskStatuses.SUCCESS,
        RetryTaskStatuses.CANCELLED,
        RetryTaskStatuses.REQUEUED,
        RetryTaskStatuses.CLEANUP,
    }

    logger.info("Cleaning up tasks created before %s...", time_reference.date())
    db_session: "Session"
    with SyncSessionMaker() as db_session:
        res = db_session.execute(
            RetryTask.__table__.delete().where(
                RetryTask.status.in_(deleteable_task_statuses),
                RetryTask.created_at < time_reference,
            )
        )
        db_session.commit()

    logger.info("Deleted %d tasks. ( °╭ ︿ ╮°)", res.rowcount)
