from typing import TYPE_CHECKING
from uuid import UUID

from retry_tasks_lib.db.models import RetryTask, TaskType, TaskTypeKey, TaskTypeKeyValue
from retry_tasks_lib.enums import RetryTaskStatuses
from sqlalchemy import select
from sqlalchemy.orm import aliased, joinedload

from cosmos.db.base_class import async_run_query
from cosmos.db.models import AccountHolder, Reward

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.ext.asyncio import AsyncSession


async def commit(db_session: "AsyncSession") -> None:
    async def _persist() -> None:
        return await db_session.commit()

    await async_run_query(_persist, db_session, rollback_on_exc=False)


async def get_reward(db_session: "AsyncSession", reward_uuid: UUID, retailer_id: int) -> Reward | None:
    async def _query() -> Reward | None:
        ah_alias = aliased(AccountHolder)
        return (
            await db_session.execute(
                select(Reward)
                .options(joinedload(Reward.account_holder.of_type(ah_alias)), joinedload(Reward.reward_config))
                .where(
                    Reward.reward_uuid == reward_uuid,
                    ah_alias.retailer_id == retailer_id,
                    ah_alias.id == Reward.account_holder_id,
                    Reward.issued_date.is_not(None),
                )
            )
        ).scalar_one_or_none()

    return await async_run_query(_query, db_session, rollback_on_exc=False)


async def get_waiting_retry_tasks_by_key_value_in(
    db_session: "AsyncSession", task_type_name: str, key_name: str, key_values: list[str]
) -> "Sequence[RetryTask]":
    async def _query() -> "Sequence[RetryTask]":
        return (
            (
                await db_session.execute(
                    select(RetryTask)
                    .options(joinedload(RetryTask.task_type_key_values).joinedload(TaskTypeKeyValue.task_type_key))
                    .where(
                        RetryTask.status == RetryTaskStatuses.WAITING,
                        RetryTask.task_type_id == TaskType.task_type_id,
                        RetryTask.retry_task_id == TaskTypeKeyValue.retry_task_id,
                        TaskTypeKeyValue.task_type_key_id == TaskTypeKey.task_type_key_id,
                        TaskType.name == task_type_name,
                        TaskTypeKey.name == key_name,
                        TaskTypeKeyValue.value.in_(key_values),
                    )
                )
            )
            .unique()
            .scalars()
            .all()
        )

    return await async_run_query(_query, db_session, rollback_on_exc=False)
