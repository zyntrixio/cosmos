from typing import AsyncGenerator

from fastapi import Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from cosmos.db.models import Retailer
from cosmos.db.session import AsyncSessionMaker
from cosmos.retailers.crud import get_retailer_by_slug


async def get_session() -> AsyncGenerator:
    session = AsyncSessionMaker()
    try:
        yield session
    finally:
        await session.close()


class RetailerDependency:
    def __init__(self, join_active_campaign_data: bool = False, no_retailer_found_exc: Exception | None = None) -> None:
        self.no_retailer_found_exc = no_retailer_found_exc
        self.join_campaign_data = join_active_campaign_data

    async def __call__(self, retailer_slug: str, db_session: AsyncSession = Depends(get_session)) -> Retailer | None:
        retailer = await get_retailer_by_slug(
            db_session, retailer_slug=retailer_slug, with_campaign_data=self.join_campaign_data
        )
        if retailer is None and self.no_retailer_found_exc:
            raise self.no_retailer_found_exc
        return retailer
