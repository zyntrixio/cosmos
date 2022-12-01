from typing import Any

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from cosmos.core.api.deps import RetailerDependency, get_session
from cosmos.core.api.http_error import HttpErrors
from cosmos.core.api.service_result import handle_service_result
from cosmos.db.models import Retailer
from cosmos.transactions.api.deps import user_is_authorised
from cosmos.transactions.api.schemas import CreateTransactionSchema
from cosmos.transactions.api.service import TransactionService

router = APIRouter(dependencies=[Depends(user_is_authorised)])


get_retailer = RetailerDependency(
    no_retailer_found_exc=HttpErrors.INVALID_RETAILER.value, join_active_campaign_data=True
)


@router.post(
    path="/{retailer_slug}/transaction",
    response_model=str,
)
# pylint: disable=too-many-locals
async def process_transaction(
    payload: CreateTransactionSchema,
    retailer: Retailer = Depends(get_retailer),
    db_session: AsyncSession = Depends(get_session),
) -> Any:
    service = TransactionService(db_session=db_session, retailer=retailer)
    service_result = await service.handle_incoming_transaction(payload)
    return handle_service_result(service_result)
