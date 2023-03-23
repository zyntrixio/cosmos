from unittest import mock

import pytest

from wtforms import Field, Form
from wtforms.validators import StopValidation

from admin.views.retailer.validators import validate_required_fields_values_yaml


@pytest.fixture()
def mock_form() -> mock.MagicMock:
    return mock.MagicMock(spec=Form)


@pytest.fixture()
def mock_field() -> mock.MagicMock:
    return mock.MagicMock(spec=Field)


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
