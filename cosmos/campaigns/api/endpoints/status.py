from typing import Any

from fastapi import status

from cosmos.campaigns.api import api_router


@api_router.post(
    path="/{retailer_slug}/campaigns/status_change",
    status_code=status.HTTP_200_OK,
    # dependencies=[Depends(user_is_authorised)],
)
async def chamge_campaign_status() -> Any:
    return {}
