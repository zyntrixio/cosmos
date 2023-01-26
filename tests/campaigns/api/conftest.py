from typing import TYPE_CHECKING

import pytest

from fastapi.testclient import TestClient
from pytest_mock import MockerFixture

from cosmos.campaigns.api.app import app

if TYPE_CHECKING:
    from unittest.mock import MagicMock


@pytest.fixture(scope="session")
def test_client() -> TestClient:
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture(scope="function")
def mock_activity(mocker: MockerFixture) -> "MagicMock":
    return mocker.patch("cosmos.core.api.service.format_and_send_activity_in_background")
