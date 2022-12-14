from typing import Any

from fastapi import APIRouter, Depends, Header, Request
from sqlalchemy.ext.asyncio import AsyncSession

from cosmos.accounts.api.deps import bpl_channel_header_is_populated, user_is_authorised
from cosmos.accounts.api.schemas import (
    AccountHolderResponseSchema,
    AccountHolderStatusResponseSchema,
    AccountHolderUpdateStatusSchema,
    AccountHolderUUIDValidator,
    GetAccountHolderByCredentials,
)
from cosmos.accounts.api.service import AccountService
from cosmos.core.api.deps import RetailerDependency, get_session
from cosmos.core.api.service import ServiceException, handle_service_result
from cosmos.core.error_codes import ErrorCode
from cosmos.db.models import Retailer

router = APIRouter(
    prefix="/loyalty",
    dependencies=[Depends(user_is_authorised)],
)
bpl_operations_router = APIRouter(
    prefix="/loyalty",
    dependencies=[Depends(user_is_authorised), Depends(bpl_channel_header_is_populated)],
)

get_retailer = RetailerDependency(no_retailer_found_exc=ServiceException(ErrorCode.INVALID_RETAILER))


@router.post(
    path="/{retailer_slug}/accounts/getbycredentials",
    response_model=AccountHolderResponseSchema,
)
async def get_account_holder_by_credentials(
    payload: GetAccountHolderByCredentials,
    tx_qty: int | None = None,
    bpl_user_channel: str = Header(None),
    retailer: Retailer = Depends(get_retailer),
    db_session: AsyncSession = Depends(get_session),
) -> Any:
    service = AccountService(db_session=db_session, retailer=retailer)
    service_result = await service.handle_account_auth(payload, tx_qty=tx_qty, channel=bpl_user_channel)
    return handle_service_result(service_result)


@router.get(
    path="/{retailer_slug}/accounts/{account_holder_uuid}",
    response_model=AccountHolderResponseSchema,
)
@bpl_operations_router.get(
    path="/{retailer_slug}/accounts/{account_holder_uuid}/status",
    response_model=AccountHolderStatusResponseSchema,
)
async def get_account_holder(
    account_holder_uuid: AccountHolderUUIDValidator,
    request: Request,
    tx_qty: int | None = 10,
    retailer: Retailer = Depends(get_retailer),
    db_session: AsyncSession = Depends(get_session),
) -> Any:
    service = AccountService(db_session=db_session, retailer=retailer)
    service_result = await service.handle_get_account(
        account_holder_uuid=account_holder_uuid, tx_qty=tx_qty, is_status_request=request.url.path.endswith("/status")
    )
    return handle_service_result(service_result)


@bpl_operations_router.patch(path="/{retailer_slug}/accounts/{account_holder_uuid}/status")
async def patch_account_holder_status(
    account_holder_uuid: AccountHolderUUIDValidator,
    payload: AccountHolderUpdateStatusSchema,
    retailer: Retailer = Depends(get_retailer),
    db_session: AsyncSession = Depends(get_session),
) -> Any:
    service = AccountService(db_session=db_session, retailer=retailer)
    service_result = await service.handle_update_account_holder_status(
        account_holder_uuid=account_holder_uuid, request_payload=payload
    )
    return handle_service_result(service_result)
