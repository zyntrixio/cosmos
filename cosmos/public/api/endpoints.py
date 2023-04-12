import logging

from typing import TYPE_CHECKING, Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import parse_obj_as
from sqlalchemy.ext.asyncio import AsyncSession

from cosmos.core.api.deps import get_session
from cosmos.public.api.schemas import AccountHolderEmailEvent, RewardMicrositeResponseSchema
from cosmos.public.api.service import CallbackService, PublicService
from cosmos.public.config import public_settings

if TYPE_CHECKING:
    from cosmos.db.models import Reward

logger = logging.getLogger(__name__)

public_router = APIRouter(prefix=public_settings.PUBLIC_API_PREFIX)
security = HTTPBasic()


def validate_mailjet_credentials(credentials: Annotated[HTTPBasicCredentials, Depends(security)]) -> None:
    if (
        credentials.username != public_settings.MAIL_EVENT_CALLBACK_USERNAME
        or credentials.password != public_settings.MAIL_EVENT_CALLBACK_PASSWORD
    ):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED)


@public_router.get(
    path="/{retailer_slug}/marketing/unsubscribe",
    status_code=status.HTTP_202_ACCEPTED,
    response_class=HTMLResponse,
)
async def opt_out_marketing_preferences(
    retailer_slug: str,
    db_session: Annotated[AsyncSession, Depends(get_session)],
    u: str | None = None,
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
    db_session: Annotated[AsyncSession, Depends(get_session)],
) -> "Reward":
    service = PublicService(db_session=db_session, retailer_slug=retailer_slug)
    service_result = await service.handle_get_reward_for_microsite(reward_uuid)
    return service_result.handle_service_result()


@public_router.post(path="/email/event", dependencies=[Depends(validate_mailjet_credentials)])
async def account_holder_email_callback_event(
    payload: dict, db_session: Annotated[AsyncSession, Depends(get_session)]
) -> dict:
    try:
        parsed_payload = parse_obj_as(AccountHolderEmailEvent, payload)
    except Exception:
        logger.exception("failed to parse payload %s", payload)
        raise

    service = CallbackService(db_session=db_session)
    service_result = await service.handle_email_event(payload=parsed_payload)
    return service_result.handle_service_result()
