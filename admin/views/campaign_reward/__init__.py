from typing import TYPE_CHECKING

from admin.db.session import db_session
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

if TYPE_CHECKING:
    from flask_admin import Admin


def register_campaign_and_reward_management_admin(admin: "Admin") -> None:
    campaign_and_reward_management_title = "Campaign and Reward"
    admin.add_view(
        CampaignAdmin(
            Campaign,
            db_session,
            "Campaigns",
            endpoint=f"{settings.CAMPAIGN_AND_REWARD_MENU_PREFIX}/campaigns",
            category=campaign_and_reward_management_title,
        )
    )
    admin.add_view(
        EarnRuleAdmin(
            EarnRule,
            db_session,
            "Earn Rules",
            endpoint=f"{settings.CAMPAIGN_AND_REWARD_MENU_PREFIX}/earn-rules",
            category=campaign_and_reward_management_title,
        )
    )
    admin.add_view(
        RewardRuleAdmin(
            RewardRule,
            db_session,
            "Reward Rules",
            endpoint=f"{settings.CAMPAIGN_AND_REWARD_MENU_PREFIX}/reward-rules",
            category=campaign_and_reward_management_title,
        )
    )
    admin.add_view(
        RewardConfigAdmin(
            RewardConfig,
            db_session,
            "Reward Configurations",
            endpoint=f"{settings.CAMPAIGN_AND_REWARD_MENU_PREFIX}/reward-configs",
            category=campaign_and_reward_management_title,
        )
    )
    admin.add_view(
        RewardAdmin(
            Reward,
            db_session,
            "Rewards",
            endpoint=f"{settings.CAMPAIGN_AND_REWARD_MENU_PREFIX}/rewards",
            category=campaign_and_reward_management_title,
        )
    )
    admin.add_view(
        ReadOnlyRewardAdmin(
            Reward,
            db_session,
            "Rewards",
            endpoint=f"{settings.CAMPAIGN_AND_REWARD_MENU_PREFIX}/ro-rewards",
            category=campaign_and_reward_management_title,
        )
    )
    admin.add_view(
        FetchTypeAdmin(
            FetchType,
            db_session,
            "Fetch Types",
            endpoint=f"{settings.CAMPAIGN_AND_REWARD_MENU_PREFIX}/fetch-types",
            category=campaign_and_reward_management_title,
        )
    )
    admin.add_view(
        RewardUpdateAdmin(
            RewardUpdate,
            db_session,
            "Reward Updates",
            endpoint=f"{settings.CAMPAIGN_AND_REWARD_MENU_PREFIX}/reward-updates",
            category=campaign_and_reward_management_title,
        )
    )
    admin.add_view(
        RewardFileLogAdmin(
            RewardFileLog,
            db_session,
            "Reward File Log",
            endpoint=f"{settings.CAMPAIGN_AND_REWARD_MENU_PREFIX}/reward-file-log",
            category=campaign_and_reward_management_title,
        )
    )
