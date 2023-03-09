import pytest

from sqlalchemy.exc import IntegrityError

from cosmos.db.models import Retailer
from cosmos.retailers.enums import RetailerStatuses
from tests.conftest import SetupType


@pytest.mark.parametrize("params", [[0, None], [None, 0]])
def test_balance_lifespan_check_constraint(setup: SetupType, params: list) -> None:
    balance_lifespan, warning_days = params
    db_session, _, _ = setup
    retailer = Retailer(
        name="Test Retailer",
        slug="test-retailer",
        account_number_prefix="TEST",
        profile_config="""email:
  required: true
  label: Email address
first_name:
  required: true
  label: Forename
last_name:
  required: true
  label: Surname""",
        marketing_preference_config="""marketing_pref:
  label: Spam?
  type: boolean""",
        loyalty_name="test",
        status=RetailerStatuses.TEST,
        balance_lifespan=balance_lifespan,
        balance_reset_advanced_warning_days=warning_days,
    )
    with pytest.raises(IntegrityError) as exc_info:
        db_session.add(retailer)
        db_session.commit()
    assert "violates check constraint" in exc_info.value.args[0]
