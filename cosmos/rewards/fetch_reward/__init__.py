from importlib import import_module
from typing import TYPE_CHECKING

from sqlalchemy.future import select

from cosmos.db.models import AccountHolder, Campaign, RetailerFetchType, RewardConfig
from cosmos.rewards.schemas import IssuanceTaskParams

from .base import BaseAgent

if TYPE_CHECKING:  # pragma: no cover
    from retry_tasks_lib.db.models import RetryTask
    from sqlalchemy.orm import Session


def issue_agent_specific_reward(
    db_session: "Session",
    *,
    campaign: Campaign,
    reward_config: RewardConfig,
    account_holder: AccountHolder,
    retry_task: "RetryTask",
    task_params: IssuanceTaskParams,
) -> str | None:
    """issues a Reward, returns the Reward's associated_url if successful None if not."""

    try:
        module_path, cls_name = reward_config.fetch_type.path.rsplit(".", 1)
        module = import_module(module_path)
        Agent: type[BaseAgent] = getattr(module, cls_name)  # noqa: N806
    except (ValueError, ModuleNotFoundError, AttributeError) as ex:
        BaseAgent.logger.warning(
            "Could not import agent class for fetch_type %s.", reward_config.fetch_type.name, exc_info=ex
        )
        raise

    retailer_fetch_type: RetailerFetchType = db_session.execute(
        select(RetailerFetchType).where(
            RetailerFetchType.retailer_id == reward_config.retailer_id,
            RetailerFetchType.fetch_type_id == reward_config.fetch_type_id,
        )
    ).scalar_one()

    with Agent(
        db_session,
        campaign=campaign,
        reward_config=reward_config,
        account_holder=account_holder,
        config=retailer_fetch_type.load_agent_config(),
        retry_task=retry_task,
        task_params=task_params,
    ) as agent:
        return agent.issue_reward()
