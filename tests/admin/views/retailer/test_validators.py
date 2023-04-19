from typing import TYPE_CHECKING
from unittest import mock

import pytest

from wtforms.validators import StopValidation

from admin.views.retailer.validators import validate_balance_reset_email_template, validate_required_fields_values_yaml

if TYPE_CHECKING:
    from collections.abc import Generator

    from pytest_mock import MockerFixture
    from sqlalchemy.orm import Session

    from cosmos.db.models import EmailTemplate, Retailer


@pytest.fixture(scope="function", autouse=True)
def override_scoped_session(db_session: "Session", mocker: "MockerFixture") -> "Generator[None, None, None]":
    with mocker.patch("admin.views.retailer.validators.scoped_db_session", db_session):
        yield


def test_validate_required_fields_values_ok(mock_form: mock.MagicMock, mock_field: mock.MagicMock) -> None:
    mock_form.email_type = mock.Mock(data=mock.Mock(required_fields="field_1: integer"))
    mock_field.data = "field_1: 15"

    validate_required_fields_values_yaml(mock_form, mock_field)


def test_validate_required_fields_values_ok_empty_string(mock_form: mock.MagicMock, mock_field: mock.MagicMock) -> None:
    mock_form.email_type = mock.Mock(data=mock.Mock(required_fields=""))
    mock_field.data = ""

    validate_required_fields_values_yaml(mock_form, mock_field)


def test_validate_required_fields_values_ok_none_value(mock_form: mock.MagicMock, mock_field: mock.MagicMock) -> None:
    mock_form.email_type = mock.Mock(data=mock.Mock(required_fields=None))
    mock_field.data = ""

    validate_required_fields_values_yaml(mock_form, mock_field)


def test_validate_required_fields_values_mismatched_keys(mock_form: mock.MagicMock, mock_field: mock.MagicMock) -> None:
    mock_form.email_type = mock.Mock(data=mock.Mock(required_fields="field_2: integer"))
    mock_field.data = "field_1: 15"

    with pytest.raises(StopValidation) as ex_info:
        validate_required_fields_values_yaml(mock_form, mock_field)

    assert ex_info.value.args[0] == "field_2: field required, field_1: extra fields not permitted"


def test_validate_required_fields_values_invalid_yaml(mock_form: mock.MagicMock, mock_field: mock.MagicMock) -> None:
    mock_form.email_type = mock.Mock(data=mock.Mock(required_fields="field_2: integer"))
    mock_field.data = "field_2_15"

    with pytest.raises(StopValidation) as ex_info:
        validate_required_fields_values_yaml(mock_form, mock_field)

    assert ex_info.value.args[0] == "The submitted YAML is not valid."


def test_validate_required_fields_values_mismatched_value(
    mock_form: mock.MagicMock, mock_field: mock.MagicMock
) -> None:
    mock_form.email_type = mock.Mock(data=mock.Mock(required_fields="field_1: integer"))
    mock_field.data = "field_1: five"

    with pytest.raises(StopValidation) as ex_info:
        validate_required_fields_values_yaml(mock_form, mock_field)

    assert ex_info.value.args[0] == "field_1: value is not a valid integer"


def test_validate_required_fields_values_missing_field(mock_form: mock.MagicMock, mock_field: mock.MagicMock) -> None:
    mock_form.email_type = mock.Mock(data=mock.Mock(required_fields="field_1: integer\nother: string"))
    mock_field.data = "field_1: 15"

    with pytest.raises(StopValidation) as ex_info:
        validate_required_fields_values_yaml(mock_form, mock_field)

    assert ex_info.value.args[0] == "other: field required"


def test_validate_required_fields_values_mismatched_one_empty(
    mock_form: mock.MagicMock, mock_field: mock.MagicMock
) -> None:
    mock_form.email_type = mock.Mock(data=mock.Mock(required_fields=None))
    mock_field.data = "field_1: 15"

    with pytest.raises(StopValidation) as ex_info:
        validate_required_fields_values_yaml(mock_form, mock_field)

    assert ex_info.value.args[0] == "'required_fields_values' must be empty for this email type."


def test_validate_balance_reset_email_template_ok_template_exists(
    mock_form: mock.MagicMock, mock_field: mock.MagicMock, balance_reset_email_template: "EmailTemplate"
) -> None:
    mock_form._obj = balance_reset_email_template.retailer
    mock_field.data = 30

    validate_balance_reset_email_template(mock_form, mock_field)


def test_validate_balance_reset_email_template_ok_field_not_set(
    mock_form: mock.MagicMock, mock_field: mock.MagicMock
) -> None:
    mock_form._obj = None
    mock_field.data = None

    validate_balance_reset_email_template(mock_form, mock_field)


def test_validate_balance_reset_email_template_missing_template(
    mock_form: mock.MagicMock, mock_field: mock.MagicMock, retailer: "Retailer"
) -> None:
    mock_form._obj = retailer
    mock_field.data = 30

    with pytest.raises(StopValidation) as ex_info:
        validate_balance_reset_email_template(mock_form, mock_field)

    assert ex_info.value.args[0] == "Balance nudge email must be configured before these values can be set"


def test_validate_balance_reset_email_template_new_retailer(
    mock_form: mock.MagicMock, mock_field: mock.MagicMock, balance_reset_email_template: "EmailTemplate"
) -> None:
    mock_form._obj = None
    mock_field.data = 30

    with pytest.raises(StopValidation) as ex_info:
        validate_balance_reset_email_template(mock_form, mock_field)

    assert ex_info.value.args[0] == "Balance nudge email must be configured before these values can be set"
