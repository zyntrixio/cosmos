from typing import TYPE_CHECKING

from admin.views.accounts import CUSTOMER_MANAGEMENT_TITLE
from admin.views.campaign_reward.campaign import CampaignAdmin, EarnRuleAdmin, RewardRuleAdmin
from admin.views.campaign_reward.reward import (
    AllocatedRewardAdmin,
    FetchTypeAdmin,
    ReadOnlyAllocatedRewardAdmin,
    ReadOnlyRewardAdmin,
    RewardAdmin,
    RewardConfigAdmin,
    RewardFileLogAdmin,
    RewardUpdateAdmin,
)
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
            endpoint="campaigns",
            category=campaign_and_reward_management_title,
        )
    )
    admin.add_view(
        EarnRuleAdmin(
            EarnRule,
            scoped_db_session,
            "Earn Rules",
            endpoint="earn-rules",
            category=campaign_and_reward_management_title,
        )
    )
    admin.add_view(
        RewardRuleAdmin(
            RewardRule,
            scoped_db_session,
            "Reward Rules",
            endpoint="reward-rules",
            category=campaign_and_reward_management_title,
        )
    )
    admin.add_view(
        RewardConfigAdmin(
            RewardConfig,
            scoped_db_session,
            "Reward Configurations",
            endpoint="reward-configs",
            category=campaign_and_reward_management_title,
        )
    )
    admin.add_view(
        RewardAdmin(
            Reward,
            scoped_db_session,
            "Rewards",
            endpoint="rewards",
            category=campaign_and_reward_management_title,
        )
    )
    admin.add_view(
        ReadOnlyRewardAdmin(
            Reward,
            scoped_db_session,
            "Rewards",
            endpoint="ro-rewards",
            category=campaign_and_reward_management_title,
        )
    )
    admin.add_view(
        AllocatedRewardAdmin(
            Reward,
            scoped_db_session,
            "Allocated Rewards",
            endpoint="account-holder-rewards",
            category=CUSTOMER_MANAGEMENT_TITLE,
        )
    )
    admin.add_view(
        ReadOnlyAllocatedRewardAdmin(
            Reward,
            scoped_db_session,
            "Account Holder Rewards",
            endpoint="ro-account-holder-rewards",
            category=CUSTOMER_MANAGEMENT_TITLE,
        )
    )
    admin.add_view(
        FetchTypeAdmin(
            FetchType,
            scoped_db_session,
            "Fetch Types",
            endpoint="fetch-types",
            category=campaign_and_reward_management_title,
        )
    )
    admin.add_view(
        RewardUpdateAdmin(
            RewardUpdate,
            scoped_db_session,
            "Reward Updates",
            endpoint="reward-updates",
            category=campaign_and_reward_management_title,
        )
    )
    admin.add_view(
        RewardFileLogAdmin(
            RewardFileLog,
            scoped_db_session,
            "Reward File Log",
            endpoint="reward-file-log",
            category=campaign_and_reward_management_title,
        )
    )
