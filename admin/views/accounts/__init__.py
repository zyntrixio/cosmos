from typing import TYPE_CHECKING

from admin.db.session import db_session
from admin.views.accounts.main import (
    AccountHolderAdmin,
    AccountHolderProfileAdmin,
    CampaignBalanceAdmin,
    MarketingPreferenceAdmin,
    PendingRewardAdmin,
)
from cosmos.core.config import settings
from cosmos.db.models import AccountHolder, AccountHolderProfile, CampaignBalance, MarketingPreference, PendingReward

if TYPE_CHECKING:
    from flask_admin import Admin


def register_customer_admin(admin: "Admin") -> None:
    customer_management_title = "Customer"
    admin.add_view(
        AccountHolderAdmin(
            AccountHolder,
            db_session,
            "Account Holders",
            endpoint=f"{settings.ACCOUNTS_MENU_PREFIX}/account-holders",
            category=customer_management_title,
        )
    )
    admin.add_view(
        AccountHolderProfileAdmin(
            AccountHolderProfile,
            db_session,
            "Profiles",
            endpoint=f"{settings.ACCOUNTS_MENU_PREFIX}/profiles",
            category=customer_management_title,
        )
    )
    admin.add_view(
        CampaignBalanceAdmin(
            CampaignBalance,
            db_session,
            "Campaign Balances",
            endpoint=f"{settings.ACCOUNTS_MENU_PREFIX}/campaign-balances",
            category=customer_management_title,
        )
    )
    admin.add_view(
        PendingRewardAdmin(
            PendingReward,
            db_session,
            "Pending Rewards",
            endpoint=f"{settings.ACCOUNTS_MENU_PREFIX}/pending-rewards",
            category=customer_management_title,
        )
    )
    admin.add_view(
        MarketingPreferenceAdmin(
            MarketingPreference,
            db_session,
            "Marketing Preferences",
            endpoint=f"{settings.ACCOUNTS_MENU_PREFIX}/marketing-preferences",
            category=customer_management_title,
        )
    )
