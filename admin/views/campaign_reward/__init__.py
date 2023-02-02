from typing import TYPE_CHECKING

from admin.views.campaign_reward.campaign import CampaignAdmin, EarnRuleAdmin, RewardRuleAdmin
from admin.views.campaign_reward.reward import (
    FetchTypeAdmin,
    ReadOnlyRewardAdmin,
    RewardAdmin,
    RewardConfigAdmin,
    RewardFileLogAdmin,
    RewardUpdateAdmin,
)
from cosmos.core.config import settings
from cosmos.db.models import (
    Campaign,
    EarnRule,
    FetchType,
    Reward,
    RewardConfig,
    RewardFileLog,
    RewardRule,
    RewardUpdate,
)
from cosmos.db.session import scoped_db_session

if TYPE_CHECKING:
    from flask_admin import Admin


def register_campaign_and_reward_management_admin(admin: "Admin") -> None:
    campaign_and_reward_management_title = "Campaign and Reward"
    admin.add_view(
        CampaignAdmin(
            Campaign,
            scoped_db_session,
            "Campaigns",
            endpoint=f"{settings.CAMPAIGN_AND_REWARD_MENU_PREFIX}/campaigns",
            category=campaign_and_reward_management_title,
        )
    )
    admin.add_view(
        EarnRuleAdmin(
            EarnRule,
            scoped_db_session,
            "Earn Rules",
            endpoint=f"{settings.CAMPAIGN_AND_REWARD_MENU_PREFIX}/earn-rules",
            category=campaign_and_reward_management_title,
        )
    )
    admin.add_view(
        RewardRuleAdmin(
            RewardRule,
            scoped_db_session,
            "Reward Rules",
            endpoint=f"{settings.CAMPAIGN_AND_REWARD_MENU_PREFIX}/reward-rules",
            category=campaign_and_reward_management_title,
        )
    )
    admin.add_view(
        RewardConfigAdmin(
            RewardConfig,
            scoped_db_session,
            "Reward Configurations",
            endpoint=f"{settings.CAMPAIGN_AND_REWARD_MENU_PREFIX}/reward-configs",
            category=campaign_and_reward_management_title,
        )
    )
    admin.add_view(
        RewardAdmin(
            Reward,
            scoped_db_session,
            "Rewards",
            endpoint=f"{settings.CAMPAIGN_AND_REWARD_MENU_PREFIX}/rewards",
            category=campaign_and_reward_management_title,
        )
    )
    admin.add_view(
        ReadOnlyRewardAdmin(
            Reward,
            scoped_db_session,
            "Rewards",
            endpoint=f"{settings.CAMPAIGN_AND_REWARD_MENU_PREFIX}/ro-rewards",
            category=campaign_and_reward_management_title,
        )
    )
    admin.add_view(
        FetchTypeAdmin(
            FetchType,
            scoped_db_session,
            "Fetch Types",
            endpoint=f"{settings.CAMPAIGN_AND_REWARD_MENU_PREFIX}/fetch-types",
            category=campaign_and_reward_management_title,
        )
    )
    admin.add_view(
        RewardUpdateAdmin(
            RewardUpdate,
            scoped_db_session,
            "Reward Updates",
            endpoint=f"{settings.CAMPAIGN_AND_REWARD_MENU_PREFIX}/reward-updates",
            category=campaign_and_reward_management_title,
        )
    )
    admin.add_view(
        RewardFileLogAdmin(
            RewardFileLog,
            scoped_db_session,
            "Reward File Log",
            endpoint=f"{settings.CAMPAIGN_AND_REWARD_MENU_PREFIX}/reward-file-log",
            category=campaign_and_reward_management_title,
        )
    )
