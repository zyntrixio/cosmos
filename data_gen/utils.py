from random import randint
from typing import TYPE_CHECKING

from cosmos.db.models import AccountHolderCampaignBalance, Campaign

from .enums import AccountHolderTypes

if TYPE_CHECKING:
    from cosmos.db.models import AccountHolder


def generate_account_number(prefix: str, account_holder_type: AccountHolderTypes, account_holder_n: int) -> str:
    account_holder_n_str = str(account_holder_n)
    return (
        prefix
        + account_holder_type.account_holder_type_index
        + "0" * (8 - len(account_holder_n_str))
        + account_holder_n_str
        + str(randint(1, 999999))
    )


def _generate_balance(account_holder_type: AccountHolderTypes, max_val: int) -> int:

    if account_holder_type == AccountHolderTypes.ZERO_BALANCE:
        value = 0
    else:
        value = randint(1, max_val) * 100
        if account_holder_type == AccountHolderTypes.FLOAT_BALANCE:
            value += randint(1, 99)

    return value


def generate_account_holder_campaign_balances(
    account_holder: "AccountHolder",
    active_campaigns: list[Campaign],
    account_holder_type: AccountHolderTypes,
    max_val: int,
) -> list[AccountHolderCampaignBalance]:
    return [
        AccountHolderCampaignBalance(
            account_holder_id=account_holder.id,
            campaign_id=campaign.id,
            balance=_generate_balance(account_holder_type, max_val),
        )
        for campaign in active_campaigns
    ]


def generate_email(account_holder_type: AccountHolderTypes, account_holder_n: int | str) -> str:
    account_holder_n = str(account_holder_n).rjust(2, "0")
    return f"test_{account_holder_type.value}_user_{account_holder_n}@autogen.bpl"
