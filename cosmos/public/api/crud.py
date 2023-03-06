from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from cosmos.accounts.enums import MarketingPreferenceValueTypes
from cosmos.db.base_class import async_run_query
from cosmos.db.models import AccountHolder, MarketingPreference, Retailer

if TYPE_CHECKING:  # pragma: no cover
    from sqlalchemy.engine.row import Row
    from sqlalchemy.ext.asyncio.session import AsyncSessionTransaction


async def get_account_holder_and_retailer_data_by_opt_out_token(
    db_session: "AsyncSession", *, opt_out_uuid: UUID
) -> "Row | None":
    async def _query() -> "Row | None":
        return (
            await db_session.execute(
                select(
                    AccountHolder.id.label("account_holder_id"),
                    AccountHolder.account_holder_uuid,
                    Retailer.name.label("retailer_name"),
                    Retailer.slug.label("retailer_slug"),
                )
                .select_from(AccountHolder)
                .join(Retailer)
                .where(
                    AccountHolder.opt_out_token == opt_out_uuid,
                )
            )
        ).first()

    return await async_run_query(_query, db_session, rollback_on_exc=False)


async def update_boolean_marketing_preferences(
    db_session: AsyncSession, *, account_holder_id: int
) -> list[tuple[str, datetime]]:
    async def _query(savepoint: "AsyncSessionTransaction") -> list[tuple[str, datetime]]:
        updates = await db_session.execute(
            update(MarketingPreference)
            .returning(MarketingPreference.key_name, MarketingPreference.updated_at)
            .where(
                MarketingPreference.account_holder_id == account_holder_id,
                MarketingPreference.value_type == MarketingPreferenceValueTypes.BOOLEAN,
                MarketingPreference.value != "False",
            )
            .values(value="False")
        )
        await savepoint.commit()
        return [(row[0], row[1].replace(tzinfo=UTC)) for row in updates.fetchall()]

    return await async_run_query(_query, db_session)
