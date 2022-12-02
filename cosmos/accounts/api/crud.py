from typing import TYPE_CHECKING

from sqlalchemy.exc import IntegrityError

from cosmos.accounts.enums import AccountHolderStatuses
from cosmos.db.models import AccountHolder, AccountHolderMarketingPreference, AccountHolderProfile

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


class AccountExists(Exception):
    pass


async def create_account_holder(
    db_session: "AsyncSession",
    *,
    email: str,
    retailer_id: int,
    profile_data: dict,
    marketing_preferences_data: list[dict]
) -> AccountHolder:
    account_holder = AccountHolder(email=email, retailer_id=retailer_id, status=AccountHolderStatuses.PENDING)
    nested = await db_session.begin_nested()
    try:
        db_session.add(account_holder)
        await nested.commit()
    except IntegrityError:
        await nested.rollback()
        raise AccountExists  # pylint: disable=raise-missing-from

    profile = AccountHolderProfile(account_holder_id=account_holder.id, **profile_data)
    db_session.add(profile)
    marketing_preferences = [
        AccountHolderMarketingPreference(account_holder_id=account_holder.id, **mp) for mp in marketing_preferences_data
    ]
    db_session.add_all(marketing_preferences)
    return account_holder
