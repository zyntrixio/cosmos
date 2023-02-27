from collections.abc import Generator
from datetime import datetime, timedelta, timezone
from unittest import mock

import pytest
import wtforms

from flask import Flask
from flask.testing import FlaskClient

from admin.app import create_app
from admin.views.model_views import AuthorisedModelView


@pytest.fixture()
def mock_form() -> mock.MagicMock:
    return mock.MagicMock(spec=wtforms.Form)


@pytest.fixture()
def mock_field() -> mock.MagicMock:
    return mock.MagicMock(spec=wtforms.Field)


@pytest.fixture(scope="session")
def app() -> Flask:
    app = create_app(with_activities=False)
    app.config["WTF_CSRF_ENABLED"] = False
    app.config["ENV"] = "development"
    return app


@pytest.fixture(scope="function")
def test_client(app: Flask) -> Generator["FlaskClient", None, None]:
    with (
        app.app_context(),
        app.test_request_context(),
        mock.patch.object(
            AuthorisedModelView,
            "user_info",
            {
                "roles": {"Admin"},
                "exp": (datetime.now(tz=timezone.utc) + timedelta(days=1)).timestamp(),
            },
        ),
    ):
        yield app.test_client()
