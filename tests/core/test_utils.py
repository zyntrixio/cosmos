import pytest

from cosmos.core.utils import pence_integer_to_currency_string, raw_stamp_value_to_string


@pytest.mark.parametrize(
    ("value", "currency", "currency_sign", "expected"),
    (
        pytest.param(1000, "GBP", False, "10.00"),
        pytest.param(1050, "GBP", True, "£10.50"),
    ),
)
def test_pence_integer_to_currency_string(value: int, currency: str, currency_sign: bool, expected: str) -> None:
    assert pence_integer_to_currency_string(value, currency, currency_sign=currency_sign) == expected


@pytest.mark.parametrize(
    ("value", "stamp_suffix", "expected"),
    (
        pytest.param(1000, True, "10 stamps", id="w sign"),
        pytest.param(200, False, "2", id="w/o sign"),
    ),
)
def test_raw_stamp_value_to_string(value: int, stamp_suffix: bool, expected: str) -> None:
    assert raw_stamp_value_to_string(value, stamp_suffix=stamp_suffix) == expected
