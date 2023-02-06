from typing import TYPE_CHECKING

from admin.views.accounts.main import (
    AccountHolderAdmin,
    CampaignBalanceAdmin,
    MarketingPreferenceAdmin,
    PendingRewardAdmin,
    ProfileAdmin,
)
from cosmos.core.config import settings
from cosmos.db.models import AccountHolder, AccountHolderProfile, CampaignBalance, MarketingPreference, PendingReward
from cosmos.db.session import scoped_db_session

if TYPE_CHECKING:
    from flask_admin import Admin


def register_customer_admin(admin: "Admin") -> None:
    customer_management_title = "Customer"
    admin.add_view(
        AccountHolderAdmin(
            AccountHolder,
            scoped_db_session,
            "Account Holders",
            endpoint="account-holders",
            category=customer_management_title,
        )
    )
    admin.add_view(
        ProfileAdmin(
            AccountHolderProfile,
            scoped_db_session,
            "Profiles",
            endpoint="profiles",
            category=customer_management_title,
        )
    )
    admin.add_view(
        CampaignBalanceAdmin(
            CampaignBalance,
            scoped_db_session,
            "Campaign Balances",
            endpoint="campaign-balances",
            category=customer_management_title,
        )
    )
    admin.add_view(
        PendingRewardAdmin(
            PendingReward,
            scoped_db_session,
            "Pending Rewards",
            endpoint="pending-rewards",
            category=customer_management_title,
        )
    )
    admin.add_view(
        MarketingPreferenceAdmin(
            MarketingPreference,
            scoped_db_session,
            "Marketing Preferences",
            endpoint="marketing-preferences",
            category=customer_management_title,
        )
    )
