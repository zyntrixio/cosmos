from typing import TYPE_CHECKING

from fastapi.testclient import TestClient

from cosmos.core.config import settings
from cosmos.public_api.api.app import app

client = TestClient(app)
accounts_auth_headers = {"Bpl-User-Channel": "channel"}  # FIXME: Check if this is needed
test_campaign_slug = "test-campaign-slug"  # pylint: disable=invalid-name
test_campaign_slug = "test-campaign-slug"  # pylint: disable=invalid-name
