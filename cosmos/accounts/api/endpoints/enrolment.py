from typing import Annotated

from fastapi import APIRouter, Depends, Header, status
from sqlalchemy.ext.asyncio import AsyncSession

from cosmos.accounts.api.schemas import AccountHolderEnrolment
from cosmos.accounts.api.service import AccountService
from cosmos.accounts.config import account_settings
from cosmos.core.api.deps import RetailerDependency, UserIsAuthorised, bpl_channel_header_is_populated, get_session
from cosmos.db.models import Retailer

get_retailer = RetailerDependency()
user_is_authorised = UserIsAuthorised(expected_token=account_settings.ACCOUNT_API_AUTH_TOKEN)
router = APIRouter(prefix=account_settings.ACCOUNT_API_PREFIX)


@router.post(
    path="/{retailer_slug}/accounts/enrolment",
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(user_is_authorised), Depends(bpl_channel_header_is_populated)],
)
async def endpoint_accounts_enrolment(
    payload: AccountHolderEnrolment,
    db_session: Annotated[AsyncSession, Depends(get_session)],
    retailer: Annotated[Retailer, Depends(get_retailer)],
    bpl_user_channel: str = Header(None),
) -> dict:

    service = AccountService(db_session=db_session, retailer=retailer)
    service_result = await service.handle_account_enrolment(payload, channel=bpl_user_channel)
    return service_result.handle_service_result()
