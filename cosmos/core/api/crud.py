from typing import TYPE_CHECKING

from retry_tasks_lib.db.models import RetryTask
from retry_tasks_lib.utils.asynchronous import async_create_task

from cosmos.db.base_class import async_run_query

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


async def create_retry_task(db_session: "AsyncSession", task_type_name: str, params: dict) -> RetryTask:
    task = await async_create_task(task_type_name=task_type_name, db_session=db_session, params=params)
    db_session.add(task)
    await db_session.flush()  # TODO: Check to see if this gets committed in the library. If so, alter the library
    return task


async def commit(db_session: "AsyncSession") -> None:
    async def _persist() -> None:
        return await db_session.commit()

    await async_run_query(_persist, db_session)
