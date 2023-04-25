import enum

from fastapi import status
from fastapi.responses import UJSONResponse
from pydantic import BaseModel


class HttpErrorDetail(BaseModel):
    display_message: str
    code: str
    fields: list[str] | None = None
    campaigns: list[str] | None = None


class HttpError(BaseModel):
    status_code: int
    detail: HttpErrorDetail


class ErrorCodeDetails(enum.Enum):
    NO_ACTIVE_CAMPAIGNS = {
        "code": "NO_ACTIVE_CAMPAIGNS",
        "display_message": "No active campaigns found for retailer.",
    }

    INVALID_STATUS_REQUESTED = {
        "display_message": "The requested status change(s) could not be performed.",
        "code": "INVALID_STATUS_REQUESTED",
    }
    MISSING_CAMPAIGN_COMPONENTS = {
        "display_message": "the provided campaign(s) could not be made active",
        "code": "MISSING_CAMPAIGN_COMPONENTS",
    }

    NO_CAMPAIGN_FOUND = {
        "display_message": "Campaign(s) not found for provided slug(s).",
        "code": "NO_CAMPAIGN_FOUND",
    }

    def set_optional_fields(self, fields: list[str] | None = None, campaigns: list[str] | None = None) -> dict:
        new_vals = {}
        if fields:
            new_vals["fields"] = fields
        if campaigns:
            new_vals["campaigns"] = campaigns

        return HttpErrorDetail(**self.value, **new_vals).dict(exclude_unset=True)


class ErrorCode(enum.Enum):
    NO_ACCOUNT_FOUND = HttpError(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=HttpErrorDetail(
            code="NO_ACCOUNT_FOUND",
            display_message="Account not found for provided credentials.",
        ),
    )
    ACCOUNT_EXISTS = HttpError(
        status_code=status.HTTP_409_CONFLICT,
        detail=HttpErrorDetail(
            code="ACCOUNT_EXISTS",
            display_message="It appears this account already exists.",
            fields=["email"],
        ),
    )
    USER_NOT_FOUND = HttpError(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=HttpErrorDetail(
            code="USER_NOT_FOUND",
            display_message="Unknown User.",
        ),
    )
    USER_NOT_ACTIVE = HttpError(
        status_code=status.HTTP_409_CONFLICT,
        detail=HttpErrorDetail(
            code="USER_NOT_ACTIVE",
            display_message="User Account not Active.",
        ),
    )
    INVALID_TX_DATE = HttpError(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=HttpErrorDetail(
            code="INVALID_TX_DATE",
            display_message="Transaction dated before user join.",
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
    INACTIVE_RETAILER = HttpError(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=HttpErrorDetail(
            display_message="Retailer is in an inactive state.",
            code="INACTIVE_RETAILER",
        ),
    )
    NO_REWARD_FOUND = HttpError(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=HttpErrorDetail(
            display_message="Reward not found.",
            code="NO_REWARD_FOUND",
        ),
    )
    INVALID_REQUEST = HttpError(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=HttpErrorDetail(
            display_message="Request is invalid",
            code="INVALID_REQUEST",
        ),
    )
    INVALID_STATUS_REQUESTED = HttpError(
        status_code=status.HTTP_409_CONFLICT,
        detail=HttpErrorDetail(
            display_message="The requested status change could not be performed.",
            code="INVALID_STATUS_REQUESTED",
        ),
    )
    MISSING_CAMPAIGN_COMPONENTS = HttpError(
        status_code=status.HTTP_409_CONFLICT,
        detail=HttpErrorDetail(
            display_message="the provided campaign could not be made active",
            code="MISSING_CAMPAIGN_COMPONENTS",
        ),
    )
    NO_CAMPAIGN_FOUND = HttpError(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=HttpErrorDetail(
            display_message="Campaign not found for provided slug.",
            code="NO_CAMPAIGN_FOUND",
        ),
    )

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

    #         "fields": [
    #             "bpl-user-channel",
    #         ],
    #     },
    #         "fields": [
    #             "idempotency-token",
    #         ],
    #     },
