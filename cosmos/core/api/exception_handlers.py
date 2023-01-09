import logging

from typing import cast

from fastapi import Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import UJSONResponse
from starlette.exceptions import HTTPException
from starlette.status import HTTP_400_BAD_REQUEST, HTTP_422_UNPROCESSABLE_ENTITY, HTTP_500_INTERNAL_SERVER_ERROR

from cosmos.accounts.api.service import ServiceError
from cosmos.core.api.exceptions import RequestPayloadValidationError
from cosmos.core.error_codes import ErrorCode

logger = logging.getLogger(__name__)

FIELD_VALIDATION_ERROR = "FIELD_VALIDATION_ERROR"


def _format_validation_errors(payload: list[dict]) -> tuple[int, list[dict] | dict]:
    fields = []
    for error in payload:
        if error["type"] == "value_error.jsondecode":  # noqa: PLR2004
            return (
                HTTP_400_BAD_REQUEST,
                {"display_message": "Malformed request.", "code": "MALFORMED_REQUEST"},
            )

        fields.append(error["loc"][-1])

    content = {
        "display_message": "Submitted fields are missing or invalid.",
        "code": FIELD_VALIDATION_ERROR,
        "fields": fields,
    }

    return HTTP_422_UNPROCESSABLE_ENTITY, content


async def service_exception_handler(
    request: Request,  # noqa ARG001
    exc: ServiceError,
) -> UJSONResponse:
    return ErrorCode.http_exception_response(exc.error_code.name)


# customise Api HTTPException to remove "details" and handle manually raised ValidationErrors
async def http_exception_handler(
    request: Request,  # noqa ARG001
    exc: HTTPException,
) -> UJSONResponse:

    if exc.status_code == HTTP_422_UNPROCESSABLE_ENTITY and isinstance(exc.detail, list):
        status_code, content = _format_validation_errors(exc.detail)
    else:
        status_code, content = exc.status_code, exc.detail

    headers = getattr(exc, "headers", None)
    return UJSONResponse(content, status_code=status_code, headers=headers)


async def unexpected_exception_handler(
    request: Request,  # noqa ARG001
    exc: Exception,
) -> UJSONResponse:
    try:
        return UJSONResponse(
            {
                "display_message": "An unexpected system error occurred, please try again later.",
                "code": "INTERNAL_ERROR",
            },
            status_code=HTTP_500_INTERNAL_SERVER_ERROR,
        )
    finally:
        logger.exception("Unexpected System Error", exc_info=exc)


# custom exception handler for bubbling pydandic validation errors
async def payload_request_validation_error(
    request: Request, exc: RequestPayloadValidationError  # noqa ARG001
) -> Response:
    pydantic_error = exc.validation_error
    status_code, content = _format_validation_errors(cast(list[dict], pydantic_error.errors()))
    return UJSONResponse(status_code=status_code, content=content)


# customise Api RequestValidationError
async def request_validation_handler(request: Request, exc: RequestValidationError) -> Response:  # noqa ARG001
    status_code, content = _format_validation_errors(cast(list[dict], exc.errors()))
    return UJSONResponse(status_code=status_code, content=content)
