import pytest

from fastapi.testclient import TestClient

from cosmos.transactions.api.app import app


@pytest.fixture(scope="session")
def test_client() -> TestClient:
    return TestClient(app, raise_server_exceptions=False)
