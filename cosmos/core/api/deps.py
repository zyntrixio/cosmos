from typing import AsyncGenerator

from fastapi import Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from cosmos.db.base_class import async_run_query
from cosmos.db.models import Retailer
from cosmos.db.session import AsyncSessionMaker


async def get_session() -> AsyncGenerator:
    session = AsyncSessionMaker()
    try:
        yield session
    finally:
        await session.close()


async def _get_retailer(retailer_slug: str, db_session: AsyncSession = Depends(get_session)) -> Retailer:
    async def _query() -> Retailer:
        return (await db_session.execute(select(Retailer).where(Retailer.slug == retailer_slug))).scalar_one_or_none()

    retailer = await async_run_query(_query, db_session, rollback_on_exc=False)
    return retailer


class RetailerDependency:
    def __init__(self, no_retailer_found_exc: HTTPException | None = None):
        self.no_retailer_found_exc = no_retailer_found_exc

    def __call__(self, retailer: Retailer = Depends(_get_retailer)) -> Retailer:
        if retailer is None and self.no_retailer_found_exc:
            raise self.no_retailer_found_exc
        return retailer
