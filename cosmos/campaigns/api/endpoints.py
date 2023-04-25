from typing import Annotated

from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from cosmos.campaigns.api.schemas import CampaignsMigrationSchema, CampaignsStatusChangeSchema
from cosmos.campaigns.api.service import CampaignService
from cosmos.campaigns.config import campaign_settings
from cosmos.core.api.deps import RetailerDependency, UserIsAuthorised, get_session
from cosmos.db.models import Retailer

api_router = APIRouter()
user_is_authorised = UserIsAuthorised(expected_token=campaign_settings.CAMPAIGN_API_AUTH_TOKEN)
get_retailer = RetailerDependency()


@api_router.post(
    path="/{retailer_slug}/status-change",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(user_is_authorised)],
)
async def change_campaign_status(
    payload: CampaignsStatusChangeSchema,
    db_session: Annotated[AsyncSession, Depends(get_session)],
    retailer: Annotated[Retailer, Depends(get_retailer)],
) -> dict:
    service = CampaignService(db_session=db_session, retailer=retailer)
    service_result = await service.handle_status_change(payload)
    return service_result.handle_service_result()


@api_router.post(
    path="/{retailer_slug}/migration",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(user_is_authorised)],
)
async def campaign_migration(
    payload: CampaignsMigrationSchema,
    db_session: Annotated[AsyncSession, Depends(get_session)],
    retailer: Annotated[Retailer, Depends(get_retailer)],
) -> dict:
    service = CampaignService(db_session=db_session, retailer=retailer)
    service_result = await service.handle_migration(payload)
    return service_result.handle_service_result()
