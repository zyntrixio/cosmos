import pytest

from cosmos.campaigns.enums import LoyaltyTypes
from cosmos.db.models import TransactionEarn


@pytest.mark.parametrize(
    ("amount", "loyalty_type", "currency_sign", "expected"),
    (
        pytest.param(1000, LoyaltyTypes.ACCUMULATOR, True, "£10.00", id="ACC w sign"),
        pytest.param(1000, LoyaltyTypes.ACCUMULATOR, False, "10.00", id="ACC w/o sign"),
        pytest.param(100, LoyaltyTypes.STAMPS, True, "1 stamp", id="1 STAMP w sign"),
        pytest.param(200, LoyaltyTypes.STAMPS, True, "2 stamps", id="2 STAMP w sign"),
        pytest.param(100, LoyaltyTypes.STAMPS, False, "1", id="1 STAMP w/o sign"),
        pytest.param(200, LoyaltyTypes.STAMPS, False, "2", id="2 STAMP w/o sign"),
    ),
)
def test_transaction_earn_humanised_earn_amount(
    amount: int, loyalty_type: LoyaltyTypes, currency_sign: bool, expected: str
) -> None:
    txe = TransactionEarn(earn_amount=amount, loyalty_type=loyalty_type)
    assert txe.humanized_earn_amount(currency_sign=currency_sign) == expected
