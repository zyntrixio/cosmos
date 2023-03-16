from collections.abc import Callable
from typing import TYPE_CHECKING

import pytest

from retry_tasks_lib.db.models import RetryTask, TaskType
from retry_tasks_lib.utils.synchronous import sync_create_task

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


@pytest.fixture(scope="function")
def create_mock_task(db_session: "Session", reward_issuance_task_type: TaskType) -> Callable[..., RetryTask]:
    params = {
        "campaign_id": 1,
        "account_holder_id": 1,
        "reward_config_id": 1,
    }

    def _create_task(updated_params: dict | None = None) -> RetryTask:
        if not updated_params:
            updated_params = {}

        rt = sync_create_task(
            db_session,
            task_type_name=reward_issuance_task_type.name,
            params=params | updated_params,
        )
        db_session.commit()
        db_session.refresh(rt)
        return rt

    return _create_task
