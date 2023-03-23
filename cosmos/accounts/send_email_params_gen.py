"""
This module contains the functions mapped to an EmailType's send_email_params_fn field in the database.

These functions should identify which account holder qualify for the EmailType they are mapped to and
return a list of SendEmailParamsFnResponse object where each object contains the required params needed
to create a send-email retry task and the parameters needed to create an AccountHolderEmail object.

These functions should take as arguments db_session, email_type, retailer, and scheduler_tz.
Any extra needed param should be declared on the respective EmailType's required_fields field.

If any change to the function name or path is made, please reflect it in the database.
"""


from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, TypedDict

from sqlalchemy import tuple_
from sqlalchemy.future import select

from cosmos.core.utils import get_formatted_balance_by_loyalty_type
from cosmos.db.models import AccountHolder, AccountHolderEmail, Campaign, CampaignBalance

if TYPE_CHECKING:
    from zoneinfo import ZoneInfo

    from sqlalchemy.orm import Session

    from cosmos.db.models import EmailType, Retailer


class SendEmailParams(TypedDict):
    account_holder_id: int
    retailer_id: int
    template_type: str
    extra_params: dict


def get_balance_reset_nudge_params(
    *, db_session: "Session", email_type: "EmailType", retailer: "Retailer", scheduler_tz: "ZoneInfo"
) -> list[SendEmailParams]:
    """
    send_email_params_fn for EmailType of slug BALANCE_RESET

    return example:
    ```python
    [
        {
            "account_holder_id": 1,
            "retailer_id": 1,
            "template_type": "BALANCE_RESET",
            "extra_params": {
                "current_balance": "12.34",
                "balance_reset_date": "22/10/23",
                "datetime": "15:00 26/10/23",
                "campaign_slug": "test-campaign",
                "retailer_slug": "test-retailer",
                "retailer_name": "Test Retailer",
            },
        },
        ...,
    ]
    """
    advance_days: int | None = retailer.balance_reset_advanced_warning_days
    if not advance_days:
        return []

    already_processed = (
        select(AccountHolderEmail.account_holder_id, AccountHolderEmail.campaign_id)
        .join(AccountHolder)
        .where(
            AccountHolderEmail.allow_re_send.is_(False),
            AccountHolderEmail.email_type_id == email_type.id,
            AccountHolder.retailer_id == retailer.id,
        )
    ).subquery()

    eligible_accounts_data = db_session.execute(
        select(
            AccountHolder.id.label("account_holder_id"),
            CampaignBalance.balance.label("current_balance"),
            CampaignBalance.reset_date,
            Campaign.slug,
            Campaign.loyalty_type,
        )
        .select_from(CampaignBalance)
        .join(AccountHolder, AccountHolder.id == CampaignBalance.account_holder_id)
        .join(Campaign, Campaign.id == CampaignBalance.campaign_id)
        .where(
            AccountHolder.retailer_id == retailer.id,
            tuple_(CampaignBalance.account_holder_id, CampaignBalance.campaign_id).not_in(already_processed),
            CampaignBalance.reset_date == (datetime.now(tz=scheduler_tz) + timedelta(days=advance_days)).date(),
            CampaignBalance.balance > 0,
        )
    ).all()

    lookup_time = datetime.now(tz=UTC)
    return [
        {
            "account_holder_id": account_holder_id,
            "retailer_id": retailer.id,
            "template_type": email_type.slug,
            "extra_params": {
                "current_balance": get_formatted_balance_by_loyalty_type(current_balance, loyalty_type, sign=False),
                "balance_reset_date": reset_date.strftime("%d/%m/%Y"),
                "datetime": lookup_time.strftime("%H:%M %d/%m/%Y"),
                "campaign_slug": campaign_slug,
                "retailer_slug": retailer.slug,
                "retailer_name": retailer.name,
            },
        }
        for (
            account_holder_id,
            current_balance,
            reset_date,
            campaign_slug,
            loyalty_type,
        ) in eligible_accounts_data
    ]
