from datetime import UTC, datetime
from typing import TYPE_CHECKING

from retry_tasks_lib.utils.synchronous import enqueue_many_retry_tasks, sync_create_many_tasks
from sqlalchemy.future import select

from cosmos.core.config import redis_raw
from cosmos.core.scheduled_tasks.scheduler import acquire_lock, cron_scheduler
from cosmos.db.models import PendingReward, RewardRule
from cosmos.db.session import SyncSessionMaker
from cosmos.rewards.activity.enums import IssuedRewardReasons
from cosmos.rewards.config import reward_settings

from . import logger

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


def _process_batch_deletion(
    db_session: "Session", ripe_pending_rewards: list[PendingReward], reward_config_ids_map: dict
) -> list[int]:
    campaign_ids = set()
    for pr in ripe_pending_rewards:

        if pr.campaign_id not in reward_config_ids_map:
            campaign_ids.add(pr.campaign_id)

        db_session.delete(pr)

    if campaign_ids:
        reward_config_ids_map |= dict(
            db_session.execute(
                select(
                    RewardRule.campaign_id,
                    RewardRule.reward_config_id,
                ).where(RewardRule.campaign_id.in_(campaign_ids))
            ).all()
        )

    tasks = sync_create_many_tasks(
        db_session,
        task_type_name=reward_settings.REWARD_ISSUANCE_TASK_NAME,
        params_list=[
            {
                "account_holder_id": pr.account_holder_id,
                "campaign_id": pr.campaign_id,
                "reward_config_id": reward_config_ids_map[pr.campaign_id],
                "reason": IssuedRewardReasons.CONVERTED.name,
                "pending_reward_uuid": str(pr.pending_reward_uuid),
            }
            for pr in ripe_pending_rewards
        ],
    )
    tasks_ids: list[int] = [task.retry_task_id for task in tasks]
    db_session.commit()
    return tasks_ids


@acquire_lock(runner=cron_scheduler)
def process_pending_rewards() -> None:
    ripe_pending_rewards: list[PendingReward]
    processed_rewards_n = 0
    reward_config_ids_map: dict = {}
    query = (
        select(PendingReward)
        .with_for_update(skip_locked=True)
        .where(PendingReward.conversion_date <= datetime.now(tz=UTC))
        .limit(1000)
    )

    logger.info("Processing PendingRewards...")
    with SyncSessionMaker() as db_session:

        while ripe_pending_rewards := db_session.scalars(query).all():
            try:
                processed_rewards_n += len(ripe_pending_rewards)
                tasks_ids = _process_batch_deletion(db_session, ripe_pending_rewards, reward_config_ids_map)
            except Exception as ex:
                logger.exception("Failed to convert pending rewards.", exc_info=ex)
                return

            try:
                enqueue_many_retry_tasks(db_session, retry_tasks_ids=tasks_ids, connection=redis_raw)
            except Exception as ex:
                logger.exception(
                    "Failed to enqueue %s RetryTasks with ids: %r.",
                    reward_settings.REWARD_ISSUANCE_TASK_NAME,
                    tasks_ids,
                    exc_info=ex,
                )
                return

    if processed_rewards_n:
        logger.info("Completed, processed %d PendingRewards", processed_rewards_n)
    else:
        logger.info("Completed, no PendingReward to process.")
