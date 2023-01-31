from unittest import mock

import pytest
import wtforms

from flask import Flask
from flask.testing import FlaskClient

from admin.app import create_app


@pytest.fixture()
def mock_form() -> mock.MagicMock:
    return mock.MagicMock(spec=wtforms.Form)


@pytest.fixture()
def mock_field() -> mock.MagicMock:
    return mock.MagicMock(spec=wtforms.Field)


@pytest.fixture(scope="session")
def app() -> Flask:
    app = create_app()
    app.config["WTF_CSRF_ENABLED"] = False
    app.config["ENV"] = "development"
    return app


@pytest.fixture()
def test_client(app: Flask) -> FlaskClient:
    return app.test_client()
