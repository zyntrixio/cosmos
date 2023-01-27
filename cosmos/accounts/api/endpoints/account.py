from typing import TYPE_CHECKING

from fastapi import APIRouter, Depends, Header, Request
from sqlalchemy.ext.asyncio import AsyncSession

from cosmos.accounts.api.schemas import (  # AccountHolderStatusResponseSchema,; AccountHolderUpdateStatusSchema,
    AccountHolderResponseSchema,
    AccountHolderUUIDValidator,
    GetAccountHolderByCredentials,
)
from cosmos.accounts.api.service import AccountService
from cosmos.core.api.deps import RetailerDependency, UserIsAuthorised, bpl_channel_header_is_populated, get_session
from cosmos.core.api.service import ServiceError
from cosmos.core.config import settings
from cosmos.core.error_codes import ErrorCode
from cosmos.db.models import Retailer

if TYPE_CHECKING:
    from cosmos.db.models import AccountHolder

get_retailer = RetailerDependency(no_retailer_found_exc=ServiceError(ErrorCode.INVALID_RETAILER))
user_is_authorised = UserIsAuthorised(expected_token=settings.POLARIS_API_AUTH_TOKEN)

router = APIRouter(
    prefix=f"{settings.API_PREFIX}/loyalty",
    dependencies=[Depends(user_is_authorised)],
)
bpl_operations_router = APIRouter(
    prefix=f"{settings.API_PREFIX}/loyalty",
    dependencies=[Depends(user_is_authorised), Depends(bpl_channel_header_is_populated)],
)


@router.post(
    path="/{retailer_slug}/accounts/getbycredentials",
    response_model=AccountHolderResponseSchema,
)
async def get_account_holder_by_credentials(
    payload: GetAccountHolderByCredentials,
    tx_qty: int = 10,
    bpl_user_channel: str = Header(None),
    retailer: "Retailer" = Depends(get_retailer),
    db_session: AsyncSession = Depends(get_session),
) -> "AccountHolder":
    service = AccountService(db_session=db_session, retailer=retailer)
    service_result = await service.handle_account_auth(payload, tx_qty=tx_qty, channel=bpl_user_channel)
    return service_result.handle_service_result()


@router.get(
    path="/{retailer_slug}/accounts/{account_holder_uuid}",
    response_model=AccountHolderResponseSchema,
)
async def get_account_holder(
    account_holder_uuid: AccountHolderUUIDValidator,
    request: Request,
    tx_qty: int = 10,
    retailer: "Retailer" = Depends(get_retailer),
    db_session: AsyncSession = Depends(get_session),
) -> "AccountHolder":
    service = AccountService(db_session=db_session, retailer=retailer)
    service_result = await service.handle_get_account(
        account_holder_uuid=account_holder_uuid, retailer=retailer, request=request, tx_qty=tx_qty
    )
    return service_result.handle_service_result()
