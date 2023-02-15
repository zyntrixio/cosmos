from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from cosmos.core.api.deps import RetailerDependency, UserIsAuthorised, get_session
from cosmos.core.api.service import ServiceError
from cosmos.core.error_codes import ErrorCode
from cosmos.db.models import Retailer
from cosmos.transactions.api.schemas import CreateTransactionSchema
from cosmos.transactions.api.service import TransactionService
from cosmos.transactions.config import tx_settings

user_is_authorised = UserIsAuthorised(expected_token=tx_settings.VELA_API_AUTH_TOKEN)
api_router = APIRouter(dependencies=[Depends(user_is_authorised)])


get_retailer = RetailerDependency(
    no_retailer_found_exc=ServiceError(error_code=ErrorCode.INVALID_RETAILER),
    join_active_campaign_data=True,
)


@api_router.post(
    path="/{retailer_slug}",
    response_model=str,
)
async def process_transaction(
    payload: CreateTransactionSchema,
    retailer: Retailer = Depends(get_retailer),
    db_session: AsyncSession = Depends(get_session),
) -> str:
    service = TransactionService(db_session=db_session, retailer=retailer)
    service_result = await service.handle_incoming_transaction(payload)
    return service_result.handle_service_result()
