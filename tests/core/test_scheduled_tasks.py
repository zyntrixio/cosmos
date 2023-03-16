from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from retry_tasks_lib.db.models import RetryTask
from retry_tasks_lib.enums import RetryTaskStatuses

from cosmos.core.scheduled_tasks.task_cleanup import cleanup_old_tasks

if TYPE_CHECKING:
    from collections.abc import Callable

    from pytest_mock import MockerFixture
    from sqlalchemy.orm import Session


def test_cleanup_old_tasks(
    db_session: "Session", create_mock_task: "Callable[..., RetryTask]", mocker: "MockerFixture"
) -> None:

    now = datetime.now(tz=UTC)

    deletable_task = create_mock_task()
    deletable_task.status = RetryTaskStatuses.SUCCESS
    deletable_task.created_at = now - timedelta(days=181)
    deleted_task_id = deletable_task.retry_task_id

    wrong_status_task = create_mock_task()
    wrong_status_task.status = RetryTaskStatuses.FAILED
    wrong_status_task.created_at = now - timedelta(days=200)

    not_old_enough_task = create_mock_task()
    not_old_enough_task.status = RetryTaskStatuses.SUCCESS
    not_old_enough_task.created_at = now - timedelta(days=10)

    db_session.commit()

    mock_logger = mocker.patch("cosmos.core.scheduled_tasks.task_cleanup.logger")

    cleanup_old_tasks()

    logger_calls = mock_logger.info.call_args_list

    assert logger_calls[0].args == ("Cleaning up tasks created before %s...", (now - timedelta(days=6 * 30)).date())
    assert logger_calls[1].args == ("Deleted %d tasks. ( °╭ ︿ ╮°)", 1)

    db_session.expire_all()

    assert not db_session.get(RetryTask, deleted_task_id)
    assert wrong_status_task.retry_task_id
    assert not_old_enough_task.retry_task_id
