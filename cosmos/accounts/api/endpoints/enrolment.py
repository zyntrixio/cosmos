from typing import Any

from fastapi import status

from cosmos.accounts.api import api_router


@api_router.post(path="/{retailer_slug}/accounts/enrolment", status_code=status.HTTP_202_ACCEPTED)
async def enrol_account_holder() -> Any:
    return {}
