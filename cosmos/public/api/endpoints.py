import logging

from typing import TYPE_CHECKING

from fastapi import APIRouter, Depends, status
from fastapi.responses import HTMLResponse
from sqlalchemy.ext.asyncio import AsyncSession

from cosmos.core.api.deps import get_session
from cosmos.public.api.schemas import RewardMicrositeResponseSchema
from cosmos.public.api.service import PublicService
from cosmos.public.config import public_settings

if TYPE_CHECKING:
    from cosmos.db.models import Reward

logger = logging.getLogger("opt-out-marketing")

public_router = APIRouter(prefix=public_settings.PUBLIC_API_PREFIX)


@public_router.get(
    path="/{retailer_slug}/marketing/unsubscribe",
    status_code=status.HTTP_202_ACCEPTED,
    response_class=HTMLResponse,
)
async def opt_out_marketing_preferences(
    retailer_slug: str,
    u: str | None = None,
    db_session: AsyncSession = Depends(get_session),
) -> str:
    service = PublicService(db_session=db_session, retailer_slug=retailer_slug)
    service_result = await service.handle_marketing_unsubscribe(u)
    return service_result.handle_service_result()


@public_router.get(
    path="/{retailer_slug}/reward/{reward_uuid}",
    status_code=status.HTTP_200_OK,
    response_model=RewardMicrositeResponseSchema,
    response_model_exclude_none=True,
)
async def get_reward_for_micorsite(
    reward_uuid: str,
    retailer_slug: str,
    db_session: AsyncSession = Depends(get_session),
) -> "Reward":
    service = PublicService(db_session=db_session, retailer_slug=retailer_slug)
    service_result = await service.handle_get_reward_for_microsite(reward_uuid)
    return service_result.handle_service_result()
