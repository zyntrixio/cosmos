from enum import Enum

from fastapi import HTTPException, status


class HttpErrors(Enum):
    NO_ACCOUNT_FOUND = HTTPException(
        detail={
            "display_message": "Account not found for provided credentials.",
            "code": "NO_ACCOUNT_FOUND",
        },
        status_code=status.HTTP_404_NOT_FOUND,
    )
    INVALID_RETAILER = HTTPException(
        detail={
            "display_message": "Requested retailer is invalid.",
            "code": "INVALID_RETAILER",
        },
        status_code=status.HTTP_403_FORBIDDEN,
    )
    ACCOUNT_EXISTS = HTTPException(
        detail={
            "display_message": "It appears this account already exists.",
            "code": "ACCOUNT_EXISTS",
            "fields": ["email"],
        },
        status_code=status.HTTP_409_CONFLICT,
    )
    INVALID_TOKEN = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail={
            "display_message": "Supplied token is invalid.",
            "code": "INVALID_TOKEN",
        },
    )
    # MISSING_BPL_CHANNEL_HEADER = HTTPException(
    #     status_code=status.HTTP_400_BAD_REQUEST,
    #     detail={
    #         "display_message": "Submitted headers are missing or invalid.",
    #         "code": "HEADER_VALIDATION_ERROR",
    #         "fields": [
    #             "bpl-user-channel",
    #         ],
    #     },
    # )
    # MISSING_OR_INVALID_IDEMPOTENCY_TOKEN_HEADER = HTTPException(
    #     status_code=status.HTTP_400_BAD_REQUEST,
    #     detail={
    #         "display_message": "Submitted headers are missing or invalid.",
    #         "code": "HEADER_VALIDATION_ERROR",
    #         "fields": [
    #             "idempotency-token",
    #         ],
    #     },
    # )
    # INVALID_STATUS = HTTPException(
    #     status_code=status.HTTP_400_BAD_REQUEST,
    #     detail={"display_message": "Status Rejected.", "code": "INVALID_STATUS"},
    # )
    # NO_REWARD_FOUND = HTTPException(
    #     status_code=status.HTTP_404_NOT_FOUND,
    #     detail={"display_message": "Reward not found.", "code": "NO_REWARD_FOUND"},
    # )
    # NO_REWARD_SLUG_FOUND = HTTPException(
    #     status_code=status.HTTP_404_NOT_FOUND,
    #     detail={"display_message": "Reward slug not found", "code": "NO_REWARD_SLUG_FOUND"},
    # )
    # NO_CAMPAIGN_BALANCE = HTTPException(
    #     status_code=status.HTTP_409_CONFLICT,
    #     detail={"display_message": "No balance for provided campaign slug.", "code": "NO_CAMPAIGN_BALANCE"},
    # )
    # INVALID_ACCOUNT_HOLDER_STATUS = HTTPException(
    #     status_code=status.HTTP_409_CONFLICT,
    #     detail={"display_message": "Status could not be updated as requested.", "code": "STATUS_UPDATE_FAILED"},
    # )
    # INVALID_REQUEST = HTTPException(status_code=status.HTTP_404_NOT_FOUND)
