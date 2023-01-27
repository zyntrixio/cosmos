import pytest

from cosmos.db.models import Transaction


@pytest.mark.parametrize("params", [[1000, True, "£10.00"], [2057, False, "20.57"]])
def test_transaction_humanised_amount(params: list) -> None:
    amount, currency_sign, expected = params
    tx = Transaction(amount=amount)
    assert tx.humanized_transaction_amount(currency_sign=currency_sign) == expected
