from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import text
from starlette import status

from cosmos.core.api.deps import get_session

healthz_router = APIRouter()


@healthz_router.get(path="/livez")
async def livez() -> dict:
    return {}


@healthz_router.get(path="/readyz")
async def readyz(db_session: Annotated[AsyncSession, Depends(get_session)]) -> dict:
    try:
        await db_session.execute(text("SELECT 1"))
    except Exception as ex:  # noqa BLE001
        raise HTTPException(
            detail={"postgres": f"failed to connect to postgres due to error: {ex!r}"},
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        ) from None

    return {}
