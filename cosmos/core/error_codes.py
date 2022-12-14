import enum

from typing import Optional

from fastapi.responses import UJSONResponse
from pydantic import BaseModel
from starlette import status


class HttpErrorDetail(BaseModel):
    display_message: str
    code: str
    fields: Optional[list[str]]


class HttpError(BaseModel):
    status_code: int
    detail: HttpErrorDetail


class ErrorCode(enum.Enum):
    NO_ACCOUNT_FOUND = HttpError(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=HttpErrorDetail(
            code="NO_ACCOUNT_FOUND",
            display_message="Account not found for provided credentials",
        ),
    )
    ACCOUNT_EXISTS = HttpError(
        status_code=status.HTTP_409_CONFLICT,
        detail=HttpErrorDetail(
            code="ACCOUNT_EXISTS",
            display_message="It appears this account already exists",
            fields=["email"],
        ),
    )
    USER_NOT_FOUND = HttpError(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=HttpErrorDetail(
            code="USER_NOT_FOUND",
            display_message="Unknown User",
        ),
    )
    USER_NOT_ACTIVE = HttpError(
        status_code=status.HTTP_409_CONFLICT,
        detail=HttpErrorDetail(
            code="USER_NOT_ACTIVE",
            display_message="User Account not Active",
        ),
    )
    NO_ACTIVE_CAMPAIGNS = HttpError(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=HttpErrorDetail(
            code="NO_ACTIVE_CAMPAIGNS",
            display_message="No active campaigns found for retailer.",
        ),
    )
    DUPLICATE_TRANSACTION = HttpError(
        status_code=status.HTTP_409_CONFLICT,
        detail=HttpErrorDetail(
            code="DUPLICATE_TRANSACTION",
            display_message="Duplicate Transaction.",
        ),
    )
    INVALID_RETAILER = HttpError(
        status_code=status.HTTP_403_FORBIDDEN,
        detail=HttpErrorDetail(
            display_message="Requested retailer is invalid.",
            code="INVALID_RETAILER",
        ),
    )
    # INVALID_RETAILER = "INVALID_RETAILER"
    # INVALID_TOKEN = "INVALID_TOKEN"
    # INVALID_ACCOUNT_HOLDER_STATUS = "INVALID_ACCOUNT_HOLDER_STATUS"

    @classmethod
    def http_exception_response(
        cls, code: str, status_code: int | None = None, display_message: str | None = None
    ) -> UJSONResponse:
        try:
            error: HttpError = cls[code].value
            http_status, content = error.status_code, error.detail.dict(exclude_unset=True)
            if status_code:
                http_status = status_code
            if display_message:
                content["display_message"].update(display_message)
            return UJSONResponse(content=content, status_code=http_status)
        except KeyError:
            return UJSONResponse(
                {
                    "display_message": "An unexpected system error occurred, please try again later.",
                    "code": "INTERNAL_ERROR",
                },
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
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
    # INVALID_REQUEST = HTTPException(status_code=status.HTTP_404_NOT_FOUND)
