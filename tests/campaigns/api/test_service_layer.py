from collections.abc import Callable
from typing import TYPE_CHECKING
from unittest.mock import ANY

import pytest

from retry_tasks_lib.db.models import RetryTask, TaskType
from sqlalchemy.future import select

from cosmos.campaigns.api.service import CampaignService
from cosmos.db.models import AccountHolder, Campaign, PendingReward
from cosmos.rewards.config import reward_settings
from tests.conftest import SetupType

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
async def test__issue_pending_rewards_for_campaign(
    setup: SetupType,
    async_db_session: "AsyncSession",
    campaign_with_rules: Campaign,
    create_account_holder: Callable[..., AccountHolder],
    create_pending_reward: Callable[..., PendingReward],
    reward_issuance_task_type: TaskType,
) -> None:
    sync_db_session, retailer, account_holder_a = setup
    account_holder_b = create_account_holder(email="botato@bink.com")
    account_holder_c = create_account_holder(email="cotato@bink.com")
    for account_holder, count in ((account_holder_a, 1), (account_holder_b, 2), (account_holder_c, 3)):
        create_pending_reward(account_holder_id=account_holder.id, campaign=campaign_with_rules, count=count)

    service = CampaignService(db_session=async_db_session, retailer=retailer)
    tasks = await service._issue_pending_rewards_for_campaign(campaign=campaign_with_rules)
    await async_db_session.commit()

    # load tasks with sync db session so we can use lazy loading of params
    reward_issuance_tasks = (
        sync_db_session.execute(
            select(RetryTask).where(RetryTask.retry_task_id.in_([task.retry_task_id for task in tasks]))
        )
        .scalars()
        .unique()
        .all()
    )
    assert len(reward_issuance_tasks) == 6
    assert all([task.task_type.name == reward_settings.REWARD_ISSUANCE_TASK_NAME] for task in reward_issuance_tasks)
    task_params = [task.get_params() for task in reward_issuance_tasks]
    expected_task_params = [
        {
            "pending_reward_uuid": ANY,
            "account_holder_id": account_holder_a.id,
            "campaign_id": campaign_with_rules.id,
            "reward_config_id": campaign_with_rules.reward_rule.reward_config_id,
            "reason": "CONVERTED",
        },
        {
            "pending_reward_uuid": ANY,
            "account_holder_id": account_holder_b.id,
            "campaign_id": campaign_with_rules.id,
            "reward_config_id": campaign_with_rules.reward_rule.reward_config_id,
            "reason": "CONVERTED",
        },
        {
            "pending_reward_uuid": ANY,
            "account_holder_id": account_holder_b.id,
            "campaign_id": campaign_with_rules.id,
            "reward_config_id": campaign_with_rules.reward_rule.reward_config_id,
            "reason": "CONVERTED",
        },
        {
            "pending_reward_uuid": ANY,
            "account_holder_id": account_holder_c.id,
            "campaign_id": campaign_with_rules.id,
            "reward_config_id": campaign_with_rules.reward_rule.reward_config_id,
            "reason": "CONVERTED",
        },
        {
            "pending_reward_uuid": ANY,
            "account_holder_id": account_holder_c.id,
            "campaign_id": campaign_with_rules.id,
            "reward_config_id": campaign_with_rules.reward_rule.reward_config_id,
            "reason": "CONVERTED",
        },
        {
            "pending_reward_uuid": ANY,
            "account_holder_id": account_holder_c.id,
            "campaign_id": campaign_with_rules.id,
            "reward_config_id": campaign_with_rules.reward_rule.reward_config_id,
            "reason": "CONVERTED",
        },
    ]
    assert task_params == expected_task_params
