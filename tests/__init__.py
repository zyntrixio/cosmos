from typing import TYPE_CHECKING

if TYPE_CHECKING:

    from httpx import Response

    from cosmos.core.error_codes import ErrorCode


def validate_error_response(response: "Response", error: "ErrorCode") -> None:
    resp_json: dict = response.json()
    error_detail: dict = error.value.detail.dict(exclude_unset=True)

    assert response.status_code == error.value.status_code
    assert resp_json["display_message"] == error_detail["display_message"]
    assert resp_json["code"] == error_detail["code"]
