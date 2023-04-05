from typing import TYPE_CHECKING

from fastapi.testclient import TestClient

from cosmos.accounts.api.app import app
from cosmos.accounts.config import account_settings

if TYPE_CHECKING:
    from httpx import Response

client = TestClient(app)
accounts_auth_headers = {
    "Authorization": f"Token {account_settings.ACCOUNT_API_AUTH_TOKEN}",
    "Bpl-User-Channel": "channel",
}
bpl_operations_auth_headers = {"Authorization": f"Token {account_settings.ACCOUNT_API_AUTH_TOKEN}"}
test_campaign_slug = "test-campaign-slug"


def validate_error_response(response: "Response", error: dict) -> None:
    assert response.status_code == error["status_code"]
    assert response.json() == error["detail"]
