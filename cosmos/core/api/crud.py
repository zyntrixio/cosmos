from typing import TYPE_CHECKING
from uuid import UUID

from retry_tasks_lib.db.models import RetryTask
from retry_tasks_lib.utils.asynchronous import async_create_task
from sqlalchemy.future import select
from sqlalchemy.orm import aliased, joinedload

from cosmos.db.base_class import async_run_query
from cosmos.db.models import AccountHolder, Reward

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

    await async_run_query(_persist, db_session, rollback_on_exc=False)


async def get_reward(db_session: "AsyncSession", reward_uuid: UUID, retailer_id: int) -> Reward:
    async def _query() -> Reward:
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
