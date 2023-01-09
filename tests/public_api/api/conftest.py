import pytest

from fastapi.testclient import TestClient

from cosmos.public_api.api.app import create_app


@pytest.fixture(scope="session")
def test_client() -> TestClient:
    app = create_app()
    return TestClient(app)
