from typing import Any

from fastapi import APIRouter, Depends, Header
from sqlalchemy.ext.asyncio import AsyncSession

from cosmos.accounts.api.service import AccountService
from cosmos.accounts.schemas import AccountHolderResponseSchema, GetAccountHolderByCredentials
from cosmos.core.api.deps import RetailerDependency, get_session
from cosmos.core.api.http_error import HttpErrors
from cosmos.core.api.service_result import handle_service_result
from cosmos.db.models import Retailer

router = APIRouter()
bpl_operations_router = APIRouter()

get_retailer = RetailerDependency(no_retailer_found_exc=HttpErrors.INVALID_RETAILER.value)


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
    service_result = await service.handle_account_auth(payload, tx_qty=10, channel=bpl_user_channel)
    return handle_service_result(service_result)


# @router.get(
#     path="/{retailer_slug}/accounts/{account_holder_uuid}",
#     response_model=AccountHolderResponseSchema,
# )
# @bpl_operations_router.get(
#     path="/{retailer_slug}/accounts/{account_holder_uuid}/status",
#     response_model=AccountHolderResponseSchema,
#     response_model_include={"status"},
# )
# async def get_account_holder(
#     account_holder_uuid: AccountHolderUUIDValidator,
#     request: Request,
#     tx_qty: int | None = None,
#     retailer: RetailerConfig = Depends(get_retailer),
#     db_session: AsyncSession = Depends(get_session),
# ) -> Any:

#     is_status_request = request.url.path.endswith("/status")

#     account_holder = await crud.get_account_holder(
#         db_session=db_session,
#         fetch_rewards=not is_status_request,
#         account_holder_uuid=account_holder_uuid,
#         retailer_id=retailer.id,
#         raise_404_if_inactive=not is_status_request,
#     )
#     if not is_status_request:
#         setattr(account_holder, "retailer_status", retailer.status)
#         account_holder = await _process_account_holder_transaction_history(
#             db_session=db_session,
#             account_holder=account_holder,
#             tx_qty=tx_qty,
#         )
#     return account_holder


# @bpl_operations_router.patch(path="/{retailer_slug}/accounts/{account_holder_uuid}/status")
# async def patch_account_holder_status(
#     account_holder_uuid: AccountHolderUUIDValidator,
#     payload: AccountHolderUpdateStatusSchema,
#     retailer: RetailerConfig = Depends(get_retailer),
#     db_session: AsyncSession = Depends(get_session),
# ) -> Any:

#     account_holder = await crud.get_account_holder(
#         db_session,
#         account_holder_uuid=account_holder_uuid,
#         retailer_id=retailer.id,
#         fetch_rewards=False,
#         fetch_balances=False,
#         raise_404_if_inactive=False,
#     )

#     if account_holder.status == AccountHolderStatuses.INACTIVE:
#         raise HttpErrors.INVALID_ACCOUNT_HOLDER_STATUS.value
#     account_anonymisation_retry_task = await crud.update_account_holder_status(
#         db_session, account_holder=account_holder, retailer_id=retailer.id, status=payload.status
#     )
#     asyncio.create_task(enqueue_task(account_anonymisation_retry_task.retry_task_id))

#     return {}
