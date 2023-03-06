from datetime import UTC, datetime

import pytest

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from cosmos.db.models import Transaction
from tests.conftest import SetupType


@pytest.mark.parametrize("params", [[1000, True, "£10.00"], [2057, False, "20.57"]])
def test_transaction_humanised_amount(params: list) -> None:
    amount, currency_sign, expected = params
    tx = Transaction(amount=amount)
    assert tx.humanized_transaction_amount(currency_sign=currency_sign) == expected


@pytest.mark.parametrize("processed", [True, None, False])
def test_transaction_check_constraint(processed: bool | None, setup: SetupType, db_session: Session) -> None:
    db_session, retailer, account_holder = setup

    try:
        db_session.add(
            Transaction(
                account_holder_id=account_holder.id,
                retailer_id=retailer.id,
                transaction_id="id",
                amount=1000,
                mid="amid",
                datetime=datetime.now(tz=UTC),
                processed=processed,
            )
        )
    except IntegrityError:
        if processed in (True, None):
            pytest.fail(f"Unexpected integrity error (processed={processed})")
