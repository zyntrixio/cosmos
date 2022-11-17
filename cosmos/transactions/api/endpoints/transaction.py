from typing import Any

from fastapi import status

from cosmos.transactions.api import api_router


@api_router.post(path="/{retailer_slug}/transaction", status_code=status.HTTP_202_ACCEPTED)
async def process_transaction() -> Any:
    return {}
