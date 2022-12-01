from typing import AsyncGenerator

from fastapi import Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import contains_eager, joinedload

from cosmos.campaigns.enums import CampaignStatuses
from cosmos.db.base_class import async_run_query
from cosmos.db.models import Campaign, Retailer
from cosmos.db.session import AsyncSessionMaker


async def get_session() -> AsyncGenerator:
    session = AsyncSessionMaker()
    try:
        yield session
    finally:
        await session.close()


class RetailerDependency:
    def __init__(
        self, join_active_campaign_data: bool = False, no_retailer_found_exc: HTTPException | None = None
    ) -> None:
        self.no_retailer_found_exc = no_retailer_found_exc
        self.join_campaign_data = join_active_campaign_data

    async def __call__(self, retailer_slug: str, db_session: AsyncSession = Depends(get_session)) -> Retailer | None:
        async def _query() -> Retailer | None:
            stmt = select(Retailer).where(Retailer.slug == retailer_slug)
            if self.join_campaign_data:
                stmt = (
                    stmt.join(Retailer.campaigns)
                    .outerjoin(Retailer.stores)
                    .options(
                        contains_eager(Retailer.stores),
                        contains_eager(Retailer.campaigns).options(
                            joinedload(Campaign.reward_rule),
                            joinedload(Campaign.earn_rule),
                        ),
                    )
                    .where(Campaign.status == CampaignStatuses.ACTIVE)
                )
            return (await db_session.execute(stmt)).unique().scalar_one_or_none()

        retailer = await async_run_query(_query, db_session, rollback_on_exc=False)
        if retailer is None and self.no_retailer_found_exc:
            raise self.no_retailer_found_exc
        return retailer
