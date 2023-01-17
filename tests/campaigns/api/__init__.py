from typing import TYPE_CHECKING

from cosmos.core.config import settings

if TYPE_CHECKING:

    from requests import Response

    from cosmos.core.error_codes import ErrorCode


auth_headers = {"Authorization": f"Token {settings.VELA_API_AUTH_TOKEN}", "Bpl-User-Channel": "channel"}


def validate_error_response(response: "Response", error: "ErrorCode") -> None:
    resp_json: dict = response.json()
    error_detail: dict = error.value.detail.dict(exclude_unset=True)

    assert response.status_code == error.value.status_code
    assert resp_json["display_message"] == error_detail["display_message"]
    assert resp_json["code"] == error_detail["code"]
