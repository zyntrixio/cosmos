from typing import TYPE_CHECKING

from cosmos.campaigns.enums import LoyaltyTypes
from cosmos.core.utils import pence_integer_to_currency_string

if TYPE_CHECKING:
    from cosmos.transactions.api.service import AdjustmentAmount


def build_tx_history_reasons(tx_amount: int, adjustments: dict[str, "AdjustmentAmount"], currency: str) -> list[str]:
    reasons = []
    for adjustment in adjustments.values():

        fmt_tx_amount = pence_integer_to_currency_string(abs(tx_amount), currency)
        fmt_threshold = pence_integer_to_currency_string(adjustment.threshold, currency)

        match adjustment.accepted, tx_amount < 0:  # noqa: E999
            case True, True:
                reason = f"refund of {fmt_tx_amount} accepted"
            case True, False:
                reason = f"transaction amount {fmt_tx_amount} meets the required threshold {fmt_threshold}"
            case False, True:
                reason = f"refund of {fmt_tx_amount} not accepted"
            case _:
                reason = f"transaction amount {fmt_tx_amount} does no meet the required threshold {fmt_threshold}"

        reasons.append(reason)

    return reasons


def build_tx_history_earns(adjustments: dict[str, "AdjustmentAmount"], currency: str) -> list[dict[str, str]]:
    earns = []
    for adjustment in adjustments.values():
        if adjustment.loyalty_type == LoyaltyTypes.ACCUMULATOR:
            fmt_amount = pence_integer_to_currency_string(adjustment.amount or 0, currency)
        else:
            fmt_amount = str(int(adjustment.amount / 100)) if adjustment.amount else "0"

        earns.append({"value": fmt_amount, "type": adjustment.loyalty_type.name})

    return earns
