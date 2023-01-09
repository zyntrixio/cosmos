from contextlib import suppress
from typing import AsyncGenerator

from fastapi import Depends, Header, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from starlette import status

from cosmos.db.models import Retailer
from cosmos.db.session import AsyncSessionMaker
from cosmos.retailers.crud import get_retailer_by_slug


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    session = AsyncSessionMaker()
    try:
        yield session
    finally:
        await session.close()


class RetailerDependency:
    def __init__(self, join_active_campaign_data: bool = False, no_retailer_found_exc: Exception | None = None) -> None:
        self.no_retailer_found_exc = no_retailer_found_exc
        self.join_campaign_data = join_active_campaign_data

    async def __call__(self, retailer_slug: str, db_session: AsyncSession = Depends(get_session)) -> Retailer | None:
        retailer = await get_retailer_by_slug(
            db_session, retailer_slug=retailer_slug, with_campaign_data=self.join_campaign_data
        )
        if retailer is None and self.no_retailer_found_exc:
            raise self.no_retailer_found_exc
        return retailer


def get_authorization_token(authorization: str = Header(None)) -> str:
    with suppress(ValueError, AttributeError):
        token_type, token_value = authorization.split(" ")
        if token_type.lower() == "token":  # noqa: PLR2004
            return token_value

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail={
            "display_message": "Supplied token is invalid.",
            "code": "INVALID_TOKEN",
        },
    )


# user as in user of our api, not an account holder.
class UserIsAuthorised:
    def __init__(self, expected_token: str) -> None:
        self.expected_token = expected_token

    def __call__(self, token: str = Depends(get_authorization_token)) -> None:
        if token != self.expected_token:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail={
                    "display_message": "Supplied token is invalid.",
                    "code": "INVALID_TOKEN",
                },
            )


# check bpl-user-channel header is populated, for channel facing apis only.
def bpl_channel_header_is_populated(bpl_user_channel: str = Header(None)) -> None:
    if not bpl_user_channel:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "display_message": "Submitted headers are missing or invalid.",
                "code": "HEADER_VALIDATION_ERROR",
                "fields": [
                    "bpl-user-channel",
                ],
            },
        )
