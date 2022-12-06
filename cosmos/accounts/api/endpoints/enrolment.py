from typing import Any

from fastapi import APIRouter, Depends, Header, status
from sqlalchemy.ext.asyncio import AsyncSession

from cosmos.accounts.api.service import AccountService
from cosmos.accounts.schemas import AccountHolderEnrolment
from cosmos.core.api.deps import RetailerDependency, get_session
from cosmos.core.api.http_error import HttpErrors
from cosmos.core.api.service_result import handle_service_result
from cosmos.db.models import Retailer

get_retailer = RetailerDependency(no_retailer_found_exc=HttpErrors.INVALID_RETAILER.value)

router = APIRouter(prefix="/loyalty")


@router.post(path="/{retailer_slug}/accounts/enrolment", status_code=status.HTTP_202_ACCEPTED)
async def endpoint_accounts_enrolment(
    payload: AccountHolderEnrolment,
    bpl_user_channel: str = Header(None),
    retailer: Retailer = Depends(get_retailer),
    db_session: AsyncSession = Depends(get_session),
) -> Any:

    service = AccountService(db_session=db_session, retailer=retailer)
    service_result = await service.handle_account_enrolment(payload, channel=bpl_user_channel)
    return handle_service_result(service_result)
