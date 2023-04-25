import wtforms

from cosmos.db.models import AccountHolder
from cosmos.retailers.enums import RetailerStatuses


def validate_retailer_status(model: AccountHolder) -> None:
    if model.retailer.status == RetailerStatuses.INACTIVE:
        raise wtforms.ValidationError("You cannot amend any account holder information for an inactive retailer")
