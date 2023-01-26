from unittest import mock

import pytest
import wtforms


@pytest.fixture()
def mock_form() -> mock.MagicMock:
    return mock.MagicMock(spec=wtforms.Form)


@pytest.fixture()
def mock_field() -> mock.MagicMock:
    return mock.MagicMock(spec=wtforms.Field)
