from fastapi import APIRouter, BackgroundTasks, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from cosmos.campaigns.api.schemas import CampaignsStatusChangeSchema
from cosmos.campaigns.api.service import CampaignService
from cosmos.core.api.deps import RetailerDependency, UserIsAuthorised, get_session
from cosmos.core.api.service import ServiceError
from cosmos.core.config import settings
from cosmos.core.error_codes import ErrorCode
from cosmos.db.models import Retailer

api_router = APIRouter()

get_retailer = RetailerDependency(no_retailer_found_exc=ServiceError(ErrorCode.INVALID_RETAILER))
user_is_authorised = UserIsAuthorised(expected_token=settings.VELA_API_AUTH_TOKEN)


@api_router.post(
    path="/{retailer_slug}/campaigns/status_change",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(user_is_authorised)],
)
async def change_campaign_status(
    payload: CampaignsStatusChangeSchema,
    background_tasks: BackgroundTasks,
    db_session: AsyncSession = Depends(get_session),
    retailer: Retailer = Depends(get_retailer),
) -> dict:
    service = CampaignService(db_session=db_session, retailer=retailer, background_tasks=background_tasks)
    service_result = await service.handle_status_change(payload)
    return service_result.handle_service_result()
