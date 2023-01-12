# this file is excluded from coverage as there is no logic to test here beyond calling a library function.
# if in the future we add any logic worth testing, please remove this file from the coveragerc ignore list.
from typing import TYPE_CHECKING, Any, Callable

import rq

from retry_tasks_lib.utils.error_handler import handle_request_exception

from cosmos.core.config import redis_raw, settings
from cosmos.db.session import SyncSessionMaker

from . import logger

if TYPE_CHECKING:
    from inspect import Traceback


def log_internal_exception(func: Callable) -> Any:  # noqa: ANN401
    def wrapper(*args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
        try:
            return func(*args, **kwargs)
        except Exception as ex:
            logger.exception("Unexpected error occurred while running '%s'", func.__qualname__, exc_info=ex)
            raise

    return wrapper


def default_handler(
    job: rq.job.Job, exc_type: type, exc_value: Exception, traceback: "Traceback"  # noqa: ARG001
) -> Any:  # noqa: ANN401
    return True  # defer to the RQ default handler


# NOTE: Inter-dependency: If this function's name or module changes, ensure that
# it is relevantly reflected in the TaskType table
@log_internal_exception
def handle_retry_task_request_error(
    job: rq.job.Job, exc_type: type, exc_value: Exception, traceback: "Traceback"  # noqa: ARG001
) -> None:
    with SyncSessionMaker() as db_session:
        handle_request_exception(
            db_session=db_session,
            connection=redis_raw,
            backoff_base=settings.TASK_RETRY_BACKOFF_BASE,
            max_retries=settings.TASK_MAX_RETRIES,
            job=job,
            exc_value=exc_value,
        )


# NOTE: Inter-dependency: If this function's name or module changes, ensure that
# it is relevantly reflected in the TaskType table
@log_internal_exception
def handle_issue_reward_request_error(
    job: rq.job.Job, exc_type: type, exc_value: Exception, traceback: "Traceback"  # noqa: ARG001
) -> None:
    with SyncSessionMaker() as db_session:
        handle_request_exception(
            db_session=db_session,
            connection=redis_raw,
            backoff_base=settings.TASK_RETRY_BACKOFF_BASE,
            max_retries=settings.TASK_MAX_RETRIES,
            job=job,
            exc_value=exc_value,
            extra_status_codes_to_retry=[409],
        )
