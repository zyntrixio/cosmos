import json

from collections.abc import Generator
from typing import TYPE_CHECKING

import pytest

from cryptography.fernet import Fernet
from retry_tasks_lib.db.models import TaskTypeKey, TaskTypeKeyValue
from sqlalchemy.future import select

from cosmos.core.config import redis_raw
from cosmos.rewards.config import reward_settings
from cosmos.rewards.fetch_reward.jigsaw import Jigsaw

if TYPE_CHECKING:
    from retry_tasks_lib.db.models import RetryTask
    from sqlalchemy.orm import Session


@pytest.fixture(scope="function", autouse=True)
def clean_redis() -> Generator:
    redis_raw.delete(Jigsaw.REDIS_TOKEN_KEY)
    yield
    redis_raw.delete(Jigsaw.REDIS_TOKEN_KEY)


@pytest.fixture(scope="module", autouse=True)
def populate_fernet_key() -> Generator:
    setattr(reward_settings, "JIGSAW_AGENT_ENCRYPTION_KEY", Fernet.generate_key().decode())  # noqa: B010
    yield


@pytest.fixture(scope="module")
def fernet() -> Fernet:
    return Fernet(reward_settings.JIGSAW_AGENT_ENCRYPTION_KEY.encode())


@pytest.fixture(scope="function")
def set_reversal_true(db_session: "Session", jigsaw_reward_issuance_task: "RetryTask") -> None:
    db_session.execute(
        TaskTypeKeyValue.__table__.insert().values(
            value=json.dumps({"might_need_reversal": True}),
            retry_task_id=jigsaw_reward_issuance_task.retry_task_id,
            task_type_key_id=(
                select(TaskTypeKey.task_type_key_id)
                .where(
                    TaskTypeKey.task_type_id == jigsaw_reward_issuance_task.task_type_id,
                    TaskTypeKey.name == "agent_state_params_raw",
                )
                .scalar_subquery()
            ),
        )
    )
    db_session.commit()
