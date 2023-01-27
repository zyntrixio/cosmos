import pytest

from cosmos.campaigns.enums import LoyaltyTypes
from cosmos.db.models import TransactionEarn


@pytest.mark.parametrize(
    "params",
    [
        [1000, LoyaltyTypes.ACCUMULATOR, True, "£10.00"],
        [1000, LoyaltyTypes.ACCUMULATOR, False, "10.00"],
        [20, LoyaltyTypes.STAMPS, None, "20"],
    ],
)
def test_transaction_earn_humanised_earn_amount(params: list) -> None:
    amount, loyalty_type, currency_sign, expected = params
    txe = TransactionEarn(earn_amount=amount, loyalty_type=loyalty_type)
    assert txe.humanized_earn_amount(currency_sign=currency_sign) == expected
