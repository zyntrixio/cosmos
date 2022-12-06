from fastapi import Depends, Header

from cosmos.core.api.http_error import HttpErrors
from cosmos.core.config import settings


def get_authorization_token(authorization: str = Header(None)) -> str:
    try:
        token_type, token_value = authorization.split(" ")
        if token_type.lower() == "token":
            return token_value
    except (ValueError, AttributeError):
        pass

    raise HttpErrors.INVALID_TOKEN.value


# user as in user of our api, not an account holder.
def user_is_authorised(token: str = Depends(get_authorization_token)) -> None:
    if not token == settings.POLARIS_API_AUTH_TOKEN:
        raise HttpErrors.INVALID_TOKEN.value


# check bpl-user-channel header is populated, for channel facing apis only.
def bpl_channel_header_is_populated(bpl_user_channel: str = Header(None)) -> None:
    if not bpl_user_channel:
        raise HttpErrors.MISSING_BPL_CHANNEL_HEADER.value
