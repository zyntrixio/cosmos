from datetime import UTC, datetime
from typing import TYPE_CHECKING

import sentry_sdk

from retry_tasks_lib.db.models import RetryTask
from retry_tasks_lib.enums import RetryTaskStatuses
from retry_tasks_lib.utils.synchronous import enqueue_retry_task_delay, retryable_task
from sqlalchemy.future import select

from cosmos.campaigns.enums import CampaignStatuses
from cosmos.core.config import redis_raw
from cosmos.core.prometheus import task_processing_time_callback_fn, tasks_run_total
from cosmos.db.models import AccountHolder, Campaign, RewardConfig
from cosmos.db.session import SyncSessionMaker
from cosmos.rewards.config import reward_settings
from cosmos.rewards.fetch_reward import issue_agent_specific_reward
from cosmos.rewards.schemas import IssuanceTaskParams

from . import logger

if TYPE_CHECKING:  # pragma: no cover

    from sqlalchemy.orm import Session


# NOTE: Inter-dependency: If this function's name or module changes, ensure that
# it is relevantly reflected in the TaskType table
@retryable_task(db_session_factory=SyncSessionMaker, metrics_callback_fn=task_processing_time_callback_fn)
def issue_reward(retry_task: RetryTask, db_session: "Session") -> None:
    """Try to fetch and issue a reward, unless the campaign has been cancelled"""
    if reward_settings.core.ACTIVATE_TASKS_METRICS:
        tasks_run_total.labels(
            app=reward_settings.core.PROJECT_NAME, task_name=reward_settings.REWARD_ISSUANCE_TASK_NAME
        ).inc()

    task_params = IssuanceTaskParams(**retry_task.get_params())
    campaign: Campaign = db_session.execute(select(Campaign).where(Campaign.id == task_params.campaign_id)).scalar_one()

    if campaign.status == CampaignStatuses.CANCELLED:
        retry_task.update_task(db_session, status=RetryTaskStatuses.CANCELLED, clear_next_attempt_time=True)
        return

    reward_config = db_session.execute(
        select(RewardConfig).where(RewardConfig.id == task_params.reward_config_id)
    ).scalar_one()
    account_holder = db_session.execute(
        select(AccountHolder).where(AccountHolder.id == task_params.account_holder_id)
    ).scalar_one()

    if issue_agent_specific_reward(
        db_session,
        campaign=campaign,
        reward_config=reward_config,
        account_holder=account_holder,
        retry_task=retry_task,
        task_params=task_params,
    ):
        retry_task.update_task(db_session, status=RetryTaskStatuses.SUCCESS, clear_next_attempt_time=True)

    else:
        if reward_settings.MESSAGE_IF_NO_PRE_LOADED_REWARDS:
            with sentry_sdk.push_scope() as scope:
                scope.fingerprint = ["{{ default }}", "{{ message }}"]
                event_id = sentry_sdk.capture_message(
                    f"No Reward Codes Available for RewardConfig: "
                    f"{reward_config.id}, "
                    f"reward slug: {reward_config.slug} "
                    f"on {datetime.now(tz=UTC).strftime('%Y-%m-%d')}"
                )
                logger.info(f"Sentry event ID: {event_id}")

        retry_task.status = RetryTaskStatuses.WAITING
        db_session.commit()

        next_attempt_time = enqueue_retry_task_delay(
            connection=redis_raw,
            retry_task=retry_task,
            delay_seconds=reward_settings.REWARD_ISSUANCE_REQUEUE_BACKOFF_SECONDS,
        )
        logger.info(f"Next attempt time at {next_attempt_time}")
        retry_task.update_task(db_session, next_attempt_time=next_attempt_time)

    db_session.commit()
